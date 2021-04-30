#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

#include <faabric/rpc/macros.h>

namespace faabric::scheduler {
FunctionCallServer::FunctionCallServer()
  : faabric::transport::MessageEndpointServer(DEFAULT_RPC_HOST,
                                              FUNCTION_CALL_PORT)
  , scheduler(getScheduler())
{}

void FunctionCallServer::doRecv(const void* msgData, int size)
{
    throw std::runtime_error("doRecv for one message not implemented");
}

void FunctionCallServer::doRecv(const void* headerData,
                                int headerSize,
                                const void* bodyData,
                                int bodySize)
{
    int call = static_cast<int>(*static_cast<const char*>(headerData));
    switch (call) {
        case faabric::scheduler::FunctionCalls::MpiMessage:
            this->recvMpiMessage(bodyData, bodySize);
            break;
        default:
            throw std::runtime_error(
              fmt::format("Unrecognized call header: {}", call));
    }
}

void FunctionCallServer::recvMpiMessage(const void* msgData, int size)
{
    // TODO - can we avoid this copy here?
    faabric::MPIMessage mpiMsg;

    // Deserialise message string
    if (!mpiMsg.ParseFromArray(msgData, size)) {
        throw std::runtime_error("Error deserialising message");
    }

    MpiWorldRegistry& registry = getMpiWorldRegistry();
    MpiWorld& world = registry.getWorld(mpiMsg.worldid());
    world.enqueueMessage(mpiMsg);
}

/*
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
*/
}
