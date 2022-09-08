#include "faabric/proto/faabric.pb.h"
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
    uint8_t header = message.getHeader();
    switch (header) {
        case faabric::scheduler::FunctionCalls::ExecuteFunctions: {
            recvExecuteFunctions(message.udata(), message.size());
            break;
        }
        case faabric::scheduler::FunctionCalls::Unregister: {
            recvUnregister(message.udata(), message.size());
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
    uint8_t header = message.getHeader();
    switch (header) {
        case faabric::scheduler::FunctionCalls::Flush: {
            return recvFlush(message.udata(), message.size());
        }
        case faabric::scheduler::FunctionCalls::GetResources: {
            return recvGetResources(message.udata(), message.size());
        }
        case faabric::scheduler::FunctionCalls::PendingMigrations: {
            return recvPendingMigrations(message.udata(), message.size());
        }
        case faabric::scheduler::FunctionCalls::Reservation: {
            return recvReservation(message.udata(), message.size());
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
    parsedMsg.mutable_messages()->at(0).set_topologyhint("FORCE_LOCAL");
    scheduler.callFunctions(
      std::make_shared<faabric::BatchExecuteRequest>(parsedMsg));
}

void FunctionCallServer::recvUnregister(const uint8_t* buffer,
                                        size_t bufferSize)
{
    PARSE_MSG(faabric::UnregisterRequest, buffer, bufferSize)

    SPDLOG_DEBUG("Unregistering host {} for {}/{}",
                 parsedMsg.host(),
                 parsedMsg.user(),
                 parsedMsg.function());

    // Remove the host from the warm set
    scheduler.removeRegisteredHost(
      parsedMsg.host(), parsedMsg.user(), parsedMsg.function());
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
FunctionCallServer::recvPendingMigrations(const uint8_t* buffer,
                                          size_t bufferSize)
{
    PARSE_MSG(faabric::PendingMigrations, buffer, bufferSize);

    auto msgPtr = std::make_shared<faabric::PendingMigrations>(parsedMsg);

    scheduler.addPendingMigration(msgPtr);

    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message> FunctionCallServer::recvReservation(
  const uint8_t* buffer,
  size_t bufferSize)
{
    PARSE_MSG(faabric::ReservationRequest, buffer, bufferSize);
    
    auto msgPtr = std::make_shared<faabric::ReservationRequest>(parsedMsg);

    int allocated = scheduler.reserveSlots(msgPtr->slots());
    
    faabric::ReservationResponse res;
    res.set_allocatedslots(allocated);

    return std::make_unique<faabric::ReservationResponse>(res);
}

}
