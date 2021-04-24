#include <faabric/scheduler/AsyncCallServer.h>

#include <faabric/rpc/macros.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
AsyncCallServer::AsyncCallServer()
  : RPCServer(DEFAULT_RPC_HOST, ASYNC_FUNCTION_CALL_PORT)
{}

void AsyncCallServer::doStop()
{
    isShutdown = true;
    server->Shutdown();
    cq->Shutdown();
}

void AsyncCallServer::doStart(const std::string& serverAddr)
{
    // Build the server
    grpc::ServerBuilder builder;
    builder.AddListeningPort(serverAddr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    cq = builder.AddCompletionQueue();

    // Start it
    server = builder.BuildAndStart();
    isShutdown = false;
    faabric::util::getLogger()->info(
      "Async function call server listening on {}", serverAddr);

    this->handleRpcs();
}

void AsyncCallServer::handleRpcs()
{
    // This object wraps the logic behind the processing of a single request
    // It is mapped to the service (i.e. request type) and it's completion queue
    new CallData(&service, cq.get());

    void* tag;
    bool ok;

    // Block until we read a new tag from the completion queue.
    // Note - Next will return false if shutdown _and_ the queue is drained.
    // By not breaking from the loop if shutdown we effectively drain it.
    while (cq->Next(&tag, &ok)) {
        if (!isShutdown) {
            if (!ok) {
                throw std::runtime_error(
                  "Error dequeueing from gRPC completion queue");
            }
            static_cast<CallData*>(tag)->doRpc();
        }
    }
}

AsyncCallServer::CallData::CallData(AsyncRPCService::AsyncService* service,
                                    grpc::ServerCompletionQueue* cq)
  : service(service)
  , cq(cq)
  , responder(&ctx)
  , status(CREATE)
{
    this->doRpc();
}

void AsyncCallServer::CallData::doRpc()
{
    if (status == CREATE) {
        status = PROCESS;

        // Request the server to start processing MPI messages. We use the
        // memory address as unique identifier, so different CallData instances
        // can concurrently process different requests.
        this->service->RequestMPIMsg(&ctx, &msg, &responder, cq, cq, this);
    } else if (status == PROCESS) {
        // Spawn a new CallData instance to serve new clients
        // Note that we deallocate ourselves in the FINISH state
        new CallData(service, cq);

        // Actual message processing
        faabric::MPIMessage m = msg;
        MpiWorldRegistry& registry = getMpiWorldRegistry();
        MpiWorld& world = registry.getWorld(m.worldid());
        world.enqueueMessage(m);

        // Let gRPC know we are done
        status = FINISH;
        responder.Finish(response, Status::OK, this);
    } else {
        if (status != FINISH) {
            throw std::runtime_error("Unrecognized state in async RPC server");
        }

        delete this;
    }
}
}
