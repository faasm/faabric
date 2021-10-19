#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/DistributedCoordinator.h>
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
  , sync(getDistributedCoordinator())
{}

void FunctionCallServer::doAsyncRecv(int header,
                                     const uint8_t* buffer,
                                     size_t bufferSize)
{
    switch (header) {
        case faabric::scheduler::FunctionCalls::ExecuteFunctions: {
            recvExecuteFunctions(buffer, bufferSize);
            break;
        }
        case faabric::scheduler::FunctionCalls::Unregister: {
            recvUnregister(buffer, bufferSize);
            break;
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized async call header: {}", header));
        }
    }
}

std::unique_ptr<google::protobuf::Message> FunctionCallServer::doSyncRecv(
  int header,
  const uint8_t* buffer,
  size_t bufferSize)
{
    switch (header) {
        case faabric::scheduler::FunctionCalls::Flush: {
            return recvFlush(buffer, bufferSize);
        }
        case faabric::scheduler::FunctionCalls::GetResources: {
            return recvGetResources(buffer, bufferSize);
        }
        case faabric::scheduler::FunctionCalls::GroupLock: {
            return recvCoordinationLock(buffer, bufferSize);
        }
        case faabric::scheduler::FunctionCalls::GroupUnlock: {
            return recvCoordinationUnlock(buffer, bufferSize);
        }
        case faabric::scheduler::FunctionCalls::GroupNotify: {
            return recvCoordinationNotify(buffer, bufferSize);
        }
        case faabric::scheduler::FunctionCalls::GroupBarrier: {
            return recvCoordinationBarrier(buffer, bufferSize);
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized sync call header: {}", header));
        }
    }
}

std::unique_ptr<google::protobuf::Message> FunctionCallServer::recvFlush(
  const uint8_t* buffer,
  size_t bufferSize)
{
    // Clear out any cached state
    faabric::state::getGlobalState().forceClearAll(false);

    // Clear the scheduler
    scheduler.flushLocally();

    return std::make_unique<faabric::EmptyResponse>();
}

void FunctionCallServer::recvExecuteFunctions(const uint8_t* buffer,
                                              size_t bufferSize)
{
    PARSE_MSG(faabric::BatchExecuteRequest, buffer, bufferSize)

    // This host has now been told to execute these functions no matter what
    // TODO - avoid this copy
    scheduler.callFunctions(std::make_shared<faabric::BatchExecuteRequest>(msg),
                            true);
}

void FunctionCallServer::recvUnregister(const uint8_t* buffer,
                                        size_t bufferSize)
{
    PARSE_MSG(faabric::UnregisterRequest, buffer, bufferSize)

    std::string funcStr = faabric::util::funcToString(msg.function(), false);
    SPDLOG_DEBUG("Unregistering host {} for {}", msg.host(), funcStr);

    // Remove the host from the warm set
    scheduler.removeRegisteredHost(msg.host(), msg.function());
}

std::unique_ptr<google::protobuf::Message> FunctionCallServer::recvGetResources(
  const uint8_t* buffer,
  size_t bufferSize)
{
    auto response = std::make_unique<faabric::HostResources>(
      scheduler.getThisHostResources());
    return response;
}

std::unique_ptr<google::protobuf::Message>
FunctionCallServer::recvCoordinationLock(const uint8_t* buffer,
                                         size_t bufferSize)
{
    PARSE_MSG(faabric::CoordinationRequest, buffer, bufferSize)

    SPDLOG_TRACE("Receiving lock on {}", msg.groupid());
    sync.localLock(msg.groupid(), msg.groupsize());
    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message>
FunctionCallServer::recvCoordinationUnlock(const uint8_t* buffer,
                                           size_t bufferSize)
{
    PARSE_MSG(faabric::CoordinationRequest, buffer, bufferSize)

    SPDLOG_TRACE("Receiving unlock on {}", msg.groupid());
    sync.localUnlock(msg.groupid(), msg.groupsize());
    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message>
FunctionCallServer::recvCoordinationNotify(const uint8_t* buffer,
                                           size_t bufferSize)
{
    PARSE_MSG(faabric::CoordinationRequest, buffer, bufferSize)

    SPDLOG_TRACE("Receiving notify on {}", msg.groupid());
    sync.localNotify(msg.groupid(), msg.groupsize());
    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message>
FunctionCallServer::recvCoordinationBarrier(const uint8_t* buffer,
                                            size_t bufferSize)
{
    PARSE_MSG(faabric::CoordinationRequest, buffer, bufferSize)

    SPDLOG_TRACE("Receiving barrier on {}", msg.groupid());
    sync.localBarrier(msg.groupid(), msg.groupsize());
    return std::make_unique<faabric::EmptyResponse>();
}
}
