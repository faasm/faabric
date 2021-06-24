#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
FunctionCallServer::FunctionCallServer()
  : faabric::transport::MessageEndpointServer(FUNCTION_CALL_PORT)
  , scheduler(getScheduler())
{}

void FunctionCallServer::doAsyncRecv(faabric::transport::Message& header,
                                     faabric::transport::Message& body)
{
    assert(header.size() == sizeof(uint8_t));
    uint8_t call = static_cast<uint8_t>(*header.data());
    switch (call) {
        case faabric::scheduler::FunctionCalls::ExecuteFunctions: {
            recvExecuteFunctions(body);
            break;
        }
        case faabric::scheduler::FunctionCalls::Unregister: {
            recvUnregister(body);
            break;
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized async call header: {}", call));
        }
    }
}

std::unique_ptr<google::protobuf::Message> FunctionCallServer::doSyncRecv(
  faabric::transport::Message& header,
  faabric::transport::Message& body)
{
    assert(header.size() == sizeof(uint8_t));

    uint8_t call = static_cast<uint8_t>(*header.data());
    switch (call) {
        case faabric::scheduler::FunctionCalls::Flush: {
            return recvFlush(body);
        }
        case faabric::scheduler::FunctionCalls::GetResources: {
            return recvGetResources(body);
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized sync call header: {}", call));
        }
    }
}

std::unique_ptr<google::protobuf::Message> FunctionCallServer::recvFlush(
  faabric::transport::Message& body)
{
    PARSE_MSG(faabric::ResponseRequest, body.data(), body.size());

    // Clear out any cached state
    faabric::state::getGlobalState().forceClearAll(false);

    // Clear the scheduler
    scheduler.flushLocally();

    return std::make_unique<faabric::EmptyResponse>();
}

void FunctionCallServer::recvExecuteFunctions(faabric::transport::Message& body)
{
    PARSE_MSG(faabric::BatchExecuteRequest, body.data(), body.size())

    // This host has now been told to execute these functions no matter what
    scheduler.callFunctions(std::make_shared<faabric::BatchExecuteRequest>(msg),
                            true);
}

void FunctionCallServer::recvUnregister(faabric::transport::Message& body)
{
    PARSE_MSG(faabric::UnregisterRequest, body.data(), body.size())

    std::string funcStr = faabric::util::funcToString(msg.function(), false);
    SPDLOG_DEBUG("Unregistering host {} for {}", msg.host(), funcStr);

    // Remove the host from the warm set
    scheduler.removeRegisteredHost(msg.host(), msg.function());
}

std::unique_ptr<google::protobuf::Message> FunctionCallServer::recvGetResources(
  faabric::transport::Message& body)
{
    PARSE_MSG(faabric::ResponseRequest, body.data(), body.size())

    auto response = std::make_unique<faabric::HostResources>(
      scheduler.getThisHostResources());
    return response;
}
}
