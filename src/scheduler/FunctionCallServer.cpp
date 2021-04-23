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
FunctionCallServer::FunctionCallServer()
  : RPCServer(DEFAULT_RPC_HOST, FUNCTION_CALL_PORT)
  , scheduler(getScheduler())
{
    mpiQueue = std::make_unique<
      faabric::util::Queue<std::shared_ptr<faabric::MPIMessage>>>();
    faabric::util::getLogger()->debug("init done");
}

void FunctionCallServer::doStart(const std::string& serverAddr)
{
    // Build the server
    ServerBuilder builder;
    builder.AddListeningPort(serverAddr, InsecureServerCredentials());
    builder.RegisterService(this);

    // Set up max number of threads
    grpc::ResourceQuota rq("test-rq");
    rq.SetMaxThreads(20);
    builder.SetResourceQuota(rq);

    // Start it
    server = builder.BuildAndStart();
    faabric::util::getLogger()->info("Function call server listening on {}",
                                     serverAddr);

    server->Wait();
}

void FunctionCallServer::doStop()
{
    server->Shutdown();
}

Status FunctionCallServer::Flush(ServerContext* context,
                                 const faabric::Message* request,
                                 faabric::FunctionStatusResponse* response)
{
    auto logger = faabric::util::getLogger();

    // Clear out any cached state
    faabric::state::getGlobalState().forceClearAll(false);

    // Clear the scheduler
    scheduler.flushLocally();

    // Reset the scheduler
    scheduler.reset();

    return Status::OK;
}

Status FunctionCallServer::MPICall(ServerContext* context,
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
    /*
    this->mpiQueue->enqueue(std::make_shared<faabric::MPIMessage>(m));
    */
    PROF_END(MPICallEnqueue)

    PROF_END(MpiCall)

    return Status::OK;
}

Status FunctionCallServer::NoOp(ServerContext* context,
                                const faabric::ResourceRequest* request,
                                faabric::FunctionStatusResponse* response)
{
    return Status::OK;
}

Status FunctionCallServer::GetResources(ServerContext* context,
                                        const faabric::ResourceRequest* request,
                                        faabric::HostResources* response)
{
    *response = scheduler.getThisHostResources();

    return Status::OK;
}

Status FunctionCallServer::ExecuteFunctions(
  ServerContext* context,
  const faabric::BatchExecuteRequest* request,
  faabric::FunctionStatusResponse* response)
{
    // TODO - avoiding having to copy the message here
    faabric::BatchExecuteRequest requestCopy = *request;

    // This host has now been told to execute these functions no matter what
    scheduler.callFunctions(requestCopy, true);

    return Status::OK;
}

Status FunctionCallServer::Unregister(ServerContext* context,
                                      const faabric::UnregisterRequest* request,
                                      faabric::FunctionStatusResponse* response)
{
    std::string funcStr =
      faabric::util::funcToString(request->function(), false);
    faabric::util::getLogger()->info(
      "Unregistering host {} for {}", request->host(), funcStr);

    // Remove the host from the warm set
    scheduler.removeRegisteredHost(request->host(), request->function());
    return Status::OK;
}
}
