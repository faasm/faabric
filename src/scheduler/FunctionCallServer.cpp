#include "faabric/snapshot/SnapshotRegistry.h"
#include "faabric/util/func.h"
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

Status FunctionCallServer::PushSnapshot(
  ServerContext* context,
  const faabric::SnapshotPushRequest* request,
  faabric::FunctionStatusResponse* response)
{
    faabric::util::getLogger()->info("Pushing shapshot {} (size {})",
                                     request->key(),
                                     request->contents().size());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    faabric::util::SnapshotData data;
    data.size = request->contents().size();

    // TODO - Avoid copying. Ideally we'd use release_contents but the request
    // is const and release_xxx methods are non-const
    data.data = (uint8_t*)malloc(request->contents().size());
    std::memcpy(data.data,
                reinterpret_cast<const uint8_t*>(request->contents().data()),
                request->contents().size());

    reg.setSnapshot(request->key(), data);

    return Status::OK;
}
}
