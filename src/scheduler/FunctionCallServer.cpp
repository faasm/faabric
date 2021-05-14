#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
FunctionCallServer::FunctionCallServer()
  : faabric::transport::MessageEndpointServer(DEFAULT_FUNCTION_CALL_HOST,
                                              FUNCTION_CALL_PORT)
  , scheduler(getScheduler())
{}

void FunctionCallServer::stop()
{
    // Close the dangling scheduler endpoints
    faabric::scheduler::getScheduler().closeFunctionCallClients();

    // Call the parent stop
    MessageEndpointServer::stop(faabric::transport::getGlobalMessageContext());
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
        case faabric::scheduler::FunctionCalls::Flush:
            this->recvFlush(bodyData, bodySize);
            break;
        case faabric::scheduler::FunctionCalls::ExecuteFunctions:
            this->recvExecuteFunctions(bodyData, bodySize);
            break;
        case faabric::scheduler::FunctionCalls::Unregister:
            this->recvUnregister(bodyData, bodySize);
            break;
        case faabric::scheduler::FunctionCalls::GetResources:
            this->recvGetResources(bodyData, bodySize);
            break;
        case faabric::scheduler::FunctionCalls::SetThreadResult:
            this->recvSetThreadResult(bodyData, bodySize);
            break;
        default:
            throw std::runtime_error(
              fmt::format("Unrecognized call header: {}", call));
    }
}

void FunctionCallServer::recvMpiMessage(const void* msgData, int size)
{
    PARSE_MSG(faabric::MPIMessage, msgData, size)

    MpiWorldRegistry& registry = getMpiWorldRegistry();
    MpiWorld& world = registry.getWorld(msg.worldid());
    world.enqueueMessage(msg);
}

void FunctionCallServer::recvFlush(const void* msgData, int size)
{
    PARSE_MSG(faabric::ResponseRequest, msgData, size);

    // Clear out any cached state
    faabric::state::getGlobalState().forceClearAll(false);

    // Clear the scheduler
    scheduler.flushLocally();

    // Reset the scheduler
    scheduler.reset();
}

void FunctionCallServer::recvExecuteFunctions(const void* msgData, int size)
{
    PARSE_MSG(faabric::BatchExecuteRequest, msgData, size)

    // This host has now been told to execute these functions no matter what
    scheduler.callFunctions(
      std::make_shared<faabric::BatchExecuteRequest>(msg), true);
}

void FunctionCallServer::recvUnregister(const void* msgData, int size)
{
    PARSE_MSG(faabric::UnregisterRequest, msgData, size)

    std::string funcStr =
      faabric::util::funcToString(msg.function(), false);
    faabric::util::getLogger()->info(
      "Unregistering host {} for {}", msg.host(), funcStr);

    // Remove the host from the warm set
    scheduler.removeRegisteredHost(msg.host(), msg.function());
}

void FunctionCallServer::recvGetResources(const void* msgData, int size)
{
    PARSE_MSG(faabric::ResponseRequest, msgData, size)

    // Send the response body
    faabric::HostResources response = scheduler.getThisHostResources();
    size_t responseSize = response.ByteSizeLong();
    // Deliberately use heap-allocation for zero-copy sending
    char* serialisedMsg = new char[responseSize];
    // Serialise using protobuf
    if (!response.SerializeToArray(serialisedMsg, responseSize)) {
        throw std::runtime_error("Error serialising message");
    }
    sendResponse(
      serialisedMsg, responseSize, msg.returnhost(), FUNCTION_CALL_PORT);
}

void FunctionCallServer::recvSetThreadResult(const void* msgData, int size)
{
    PARSE_MSG(faabric::ThreadResultRequest, msgData, size)

    faabric::util::getLogger()->info("Setting thread {} result to {}",
                                     msg.messageid(),
                                     msg.returnvalue());

    scheduler.setThreadResult(msg.messageid(), msg.returnvalue());
}
}
