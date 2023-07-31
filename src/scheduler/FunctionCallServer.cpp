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
  : faabric::transport::MessageEndpointServer(
      FUNCTION_CALL_ASYNC_PORT,
      FUNCTION_CALL_SYNC_PORT,
      FUNCTION_INPROC_LABEL,
      faabric::util::getSystemConfig().functionServerThreads)
  , scheduler(getScheduler())
{}

void FunctionCallServer::doAsyncRecv(transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    switch (header) {
        case faabric::scheduler::FunctionCalls::ExecuteFunctions: {
            recvExecuteFunctions(message.udata());
            break;
        }
        case faabric::scheduler::FunctionCalls::Unregister: {
            recvUnregister(message.udata());
            break;
        }
        case faabric::scheduler::FunctionCalls::SetMessageResult: {
            recvSetMessageResult(message.udata());
            break;
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized async call header: {}", header));
        }
    }
}

std::unique_ptr<google::protobuf::Message> FunctionCallServer::doSyncRecv(
  transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    switch (header) {
        case faabric::scheduler::FunctionCalls::Flush: {
            return recvFlush(message.udata());
        }
        case faabric::scheduler::FunctionCalls::GetResources: {
            return recvGetResources(message.udata());
        }
        case faabric::scheduler::FunctionCalls::PendingMigrations: {
            return recvPendingMigrations(message.udata());
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized sync call header: {}", header));
        }
    }
}

std::unique_ptr<google::protobuf::Message> FunctionCallServer::recvFlush(
  std::span<const uint8_t> buffer)
{
    // Clear out any cached state
    faabric::state::getGlobalState().forceClearAll(false);

    // Clear the scheduler
    scheduler.flushLocally();

    return std::make_unique<faabric::EmptyResponse>();
}

void FunctionCallServer::recvExecuteFunctions(std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::BatchExecuteRequest, buffer.data(), buffer.size())

    // This host has now been told to execute these functions no matter what
    // TODO(planner-schedule): this if is only here because, temporarily, the
    // planner doesn't take any scheduling decisions
    if (!parsedMsg.comesfromplanner()) {
        parsedMsg.mutable_messages()->at(0).set_topologyhint("FORCE_LOCAL");
    } else {
        // This flags were set by the old endpoint, we temporarily set them here
        parsedMsg.mutable_messages()->at(0).set_timestamp(
          faabric::util::getGlobalClock().epochMillis());
        parsedMsg.mutable_messages()->at(0).set_masterhost(
          faabric::util::getSystemConfig().endpointHost);
    }
    scheduler.callFunctions(
      std::make_shared<faabric::BatchExecuteRequest>(parsedMsg));
}

void FunctionCallServer::recvUnregister(std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::UnregisterRequest, buffer.data(), buffer.size())

    SPDLOG_DEBUG("Unregistering host {} for {}/{}",
                 parsedMsg.host(),
                 parsedMsg.user(),
                 parsedMsg.function());

    // Remove the host from the warm set
    scheduler.removeRegisteredHost(
      parsedMsg.host(), parsedMsg.user(), parsedMsg.function());
}

std::unique_ptr<google::protobuf::Message> FunctionCallServer::recvGetResources(
  std::span<const uint8_t> buffer)
{
    auto response = std::make_unique<faabric::HostResources>(
      scheduler.getThisHostResources());
    return response;
}

std::unique_ptr<google::protobuf::Message>
FunctionCallServer::recvPendingMigrations(std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::PendingMigrations, buffer.data(), buffer.size());

    auto msgPtr = std::make_shared<faabric::PendingMigrations>(parsedMsg);

    scheduler.addPendingMigration(msgPtr);

    return std::make_unique<faabric::EmptyResponse>();
}

void FunctionCallServer::recvSetMessageResult(std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::Message, buffer.data(), buffer.size())
    faabric::planner::getPlannerClient().setMessageResultLocally(
      std::make_shared<faabric::Message>(parsedMsg));
}
}
