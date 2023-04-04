#include <faabric/planner/PlannerApi.h>
#include <faabric/planner/PlannerServer.h>
#include <faabric/transport/common.h>
#include <faabric/util/config.h>

#include <fmt/format.h>

namespace faabric::planner {
PlannerServer::PlannerServer()
  : faabric::transport::MessageEndpointServer(
      PLANNER_ASYNC_PORT,
      PLANNER_SYNC_PORT,
      PLANNER_INPROC_LABEL,
      faabric::util::getSystemConfig().plannerServerThreads)
  , planner(faabric::planner::getPlanner())
{}

void PlannerServer::doAsyncRecv(transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    switch (header) {
        // Thus far, no async calls for PlannerServer
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized async call header: {}", header));
        }
    }
}

std::unique_ptr<google::protobuf::Message> PlannerServer::doSyncRecv(
  transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    switch (header) {
        case faabric::planner::PlannerCalls::Ping: {
            return recvPing();
        }
        case faabric::planner::PlannerCalls::GetAvailableHosts: {
            return recvGetAvailableHosts(message.udata());
        }
        case faabric::planner::PlannerCalls::RegisterHost: {
            return recvRegisterHost(message.udata());
        }
        case faabric::planner::PlannerCalls::RemoveHost: {
            return recvRemoveHost(message.udata());
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized sync call header: {}", header));
        }
    }
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvPing()
{
    // Pong
    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvGetAvailableHosts(
  std::span<const uint8_t> buffer)
{

    // Send response
    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvRegisterHost(
  std::span<const uint8_t> buffer)
{

    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message>
PlannerServer::recvRemoveHost(std::span<const uint8_t> buffer)
{

    // Send response
    return std::make_unique<faabric::EmptyResponse>();
}
}
