#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/state/State.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

#include <faabric/proto/macros.h>
#include <grpcpp/grpcpp.h>

namespace faabric::scheduler {
FunctionCallServer::FunctionCallServer()
  : RPCServer(DEFAULT_RPC_HOST, FUNCTION_CALL_PORT)
  , scheduler(getScheduler())
{}

void FunctionCallServer::doStart(const std::string& serverAddr)
{
    // Build the server
    ServerBuilder builder;
    builder.AddListeningPort(serverAddr, InsecureServerCredentials());
    builder.RegisterService(this);

    // Start it
    server = builder.BuildAndStart();
    faabric::util::getLogger()->info("Function call server listening on {}",
                                     serverAddr);

    server->Wait();
}

Status FunctionCallServer::ShareFunction(
  ServerContext* context,
  const faabric::Message* request,
  faabric::FunctionStatusResponse* response)
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

    // TODO - avoiding having to copy the message here
    faabric::Message msg = *request;

    // This calls the scheduler, which will always attempt
    // to execute locally. However, if not possible, this will
    // again share the message, increasing the hops
    const std::string funcStr = faabric::util::funcToString(msg, true);
    logger->debug("{} received shared call {} (scheduled for {})",
                  host,
                  funcStr,
                  msg.scheduledhost());

    scheduler.callFunction(msg);

    return Status::OK;
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
    faabric::MPIMessage m = *request;

    MpiWorldRegistry& registry = getMpiWorldRegistry();
    MpiWorld& world = registry.getWorld(m.worldid());
    world.enqueueMessage(m);

    return Status::OK;
}

Status FunctionCallServer::GetResources(ServerContext* context,
                                        const faabric::ResourceRequest* request,
                                        faabric::HostResources* response)
{
    // Return this host's resources
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
    // Remove the host from the warm set
    scheduler.removeRegisteredHost(request->host(), request->function());
    return Status::OK;
}

}
