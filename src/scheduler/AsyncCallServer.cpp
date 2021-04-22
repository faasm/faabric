#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

#include <faabric/rpc/macros.h>
#include <grpcpp/grpcpp.h>

#include <faabric/util/timing.h>

namespace faabric::scheduler {
AsyncCallServer::AsyncCallServer()
  : RPCServer(DEFAULT_RPC_HOST, ASYNC_FUNCTION_CALL_PORT)
  , scheduler(getScheduler())
{
    // TODO remove this
    mpiQueue = std::make_unique<faabric::util::Queue<std::shared_ptr<faabric::MPIMessage>>>();
    faabric::util::getLogger()->debug("init done");
}

AsyncCallServer::doStop()
{
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
    faabric::util::getLogger()->info("Async function call server listening on {}",
                                     serverAddr);

    this->handleRpcs();
}

void AsyncCallServer::handleRpcs()
{
    new CallData(&service, cq.get());
    // Request identifier
    void *tag;
    bool ok;

    // Block until we read a new tag from the completion queue.
    // Note that the tag is the memory address of a CallData instance
    while (true) {
        if (!cq->Next(&tag, &ok) || !ok) {
            throw std::runtime_error("Error dequeueing from gRPC completion queue");
        }
        static_cast<CallData*>(tag)->doRpc();
    }
}

void AsyncCallServer::CallData::CallData(AsyncRPCService::Service* service, 
                                         grpc::ServerCompletionQueue* cq)
  : service(service), cq(cq), responder(&ctx), status(CREATE)
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
        // TODO move from here? 
        // TODO remove copy?
        faabric::MPIMessage m = *msg;
        // MpiWorldRegistry& registry = getMpiWorldRegistry();
        // MpiWorld& world = registry.getWorld(m.worldid());
        // world.enqueueMessage(m);
        this->mpiQueue->enqueue(std::make_shared<faabric::MPIMessage>(m));

        // Let gRPC know we are done
        status = FINISH;
        responder.Finish(response, Status::OK, this);
    } else {
        if (status != FINISH) {
            throw std:::runtime_error("Unrecognized state in async RPC server");
        }

        delete this;
    }
}

/*
Status AsyncCallServer::MPIMsg(ServerContext* context,
                                   const faabric::MPIMessage* request,
                                   faabric::FunctionStatusResponse* response)
{
    // TODO - avoid copying message
    PROF_START(MpiCall)

    PROF_START(MPICallCopy)
    faabric::MPIMessage m = *request;
    PROF_END(MPICallCopy)

    PROF_START(MPICallEnqueue)
    MpiWorldRegistry& registry = getMpiWorldRegistry();
    MpiWorld& world = registry.getWorld(m.worldid());
    world.enqueueMessage(m);
    // this->mpiQueue->enqueue(std::make_shared<faabric::MPIMessage>(m));
    PROF_END(MPICallEnqueue)

    PROF_END(MpiCall)

    return Status::OK;
}
*/
}
