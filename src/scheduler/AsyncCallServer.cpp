#include <faabric/scheduler/AsyncCallServer.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/util/logging.h>

#include <faabric/rpc/macros.h>

#include <faabric/util/timing.h>

#define NUM_SERVER_THREADS 10

static std::unique_ptr<
  faabric::util::Queue<std::shared_ptr<faabric::MPIMessage>>>
  mpiQueue;

namespace faabric::scheduler {
AsyncCallServer::AsyncCallServer()
  : RPCServer(DEFAULT_RPC_HOST, ASYNC_FUNCTION_CALL_PORT)
  , serverRpcContexts(NUM_SERVER_THREADS * 100)
  // , serverRpcContexts(faabric::getUsableCores() * 100)
{
    // TODO remove this
    mpiQueue = std::make_unique<
      faabric::util::Queue<std::shared_ptr<faabric::MPIMessage>>>();
    faabric::util::getLogger()->debug("init done");
}

void AsyncCallServer::doStop()
{
    {
        faabric::util::UniqueLock lock(serverMutex);
        isShutdown = true;
        server->Shutdown();
        cq->Shutdown;
    }

    // Stop all threads
    for (auto& t : serverThreads) {
        if (t.joinable()) {
            t.join();
        }
    }

    // Drain the completion queue
    void* ignoredTag;
    bool ignoredOk;
    while (cq->Next(&_tag, &ignoredOk)) {
    }
}

void AsyncCallServer::doStart(const std::string& serverAddr)
{
    // Build the server
    isShutdown = false;
    grpc::ServerBuilder builder;
    builder.AddListeningPort(serverAddr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    cq = builder.AddCompletionQueue();

    // Start it
    server = builder.BuildAndStart();
    faabric::util::getLogger()->info(
      "Async function call server listening on {}", serverAddr);

    // Initialize threads and contexts
    for (int i = 0; i < NUM_SERVER_THREADS * 100; i++) {
        refreshContext(i);
    }
    for (int i = 0; i < NUM_SERVER_THREADS; i++) {
        serverThreads.emplace_back(&faabric::scheduler::AsyncCallServer::handleRpcs, this);
    }
}

void AsyncCallServer::handleRpcs()
{
    void* tag;
    bool ok;

    // Block until we read a new tag from the completion queue.
    // Note that the tag is the memory address of a CallData instance
    while (cq->Next(&tag, &ok)) {
        if (!ok) {
            throw std::runtime_error(
              "Error dequeueing from gRPC completion queue");
        }
        int i = static_cast<int>(reinterpret_cast<intptr_t>(tag));
        switch (serverRpcContexts[i].state) {
          case RpcContext::READY:
            // Actual message processing
            // TODO move from here?
            // TODO remove copy?
            /*
            faabric::MPIMessage m = msg;
            MpiWorldRegistry& registry = getMpiWorldRegistry();
            MpiWorld& world = registry.getWorld(m.worldid());
            world.enqueueMessage(m);
            */
            mpiQueue->enqueue(std::make_shared<faabric::MPIMessage>(msg));

            // Let gRPC know we are done
            serverRpcContexts[i].state = RpcContext::DONE;
            serverRpcContexts[i].responseWriter.Finish(response, Status::OK, this);
            PROF_END(asyncRpcProcess)
            break;
          case Context::DONE:
            refreshContext(i);
            break;
        }
        // static_cast<CallData*>(tag)->doRpc();
    }
}

void AsyncCallServer::refreshContext(int i)
{
    faabric::util::UniqueLock lock(serverMutex);
    if (!isShutdown) {
        serverRpcContexts[i].state = RpcContext::READY;
        // Server context should _not_ be reused across RPCs
        // https://grpc.github.io/grpc/cpp/classgrpc_1_1_server_context.html
        serverRpcContexts[i].serverContext.reset(new grpc::ServerContext);
        serverRpcContexts[i].responseWriter.reset(
          new grpc::ServerAsyncResponseWriter<faabric::FunctionStatusResponse>(
              serverRpcContexts[i].serverContext.get()));
        service->RequestMPIMsg(serverRpcContexts[i].serverContext.get(),
                               &serverRpcContexts[i].response,
                               serverRpcContexts[i].responseWriter.get(),
                               cq.get(),
                               cq.get(),
                               reinterpret_cast<void*>(i));
    }
}

/*
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
        PROF_START(asyncRpcProcess)
        // Spawn a new CallData instance to serve new clients
        // Note that we deallocate ourselves in the FINISH state
        new CallData(service, cq);

        // Actual message processing
        // TODO move from here?
        // TODO remove copy?
        faabric::MPIMessage m = msg;
        MpiWorldRegistry& registry = getMpiWorldRegistry();
        MpiWorld& world = registry.getWorld(m.worldid());
        world.enqueueMessage(m);
        mpiQueue->enqueue(std::make_shared<faabric::MPIMessage>(msg));

        // Let gRPC know we are done
        status = FINISH;
        responder.Finish(response, Status::OK, this);
        PROF_END(asyncRpcProcess)
    } else {
        if (status != FINISH) {
            throw std::runtime_error("Unrecognized state in async RPC server");
        }

        delete this;
    }
}
*/
}
