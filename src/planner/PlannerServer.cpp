#include <faabric/planner/PlannerApi.h>
#include <faabric/planner/PlannerServer.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

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
        case PlannerCalls::RemoveHost: {
            recvRemoveHost(message.udata());
            break;
        }
        default: {
            // If we don't recognise the header, let the client fail, but don't
            // crash the planner
            SPDLOG_ERROR("Unrecognised async call header: {}", header);
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
            return recvGetAvailableHosts();
        }
        case faabric::planner::PlannerCalls::RegisterHost: {
            return recvRegisterHost(message.udata());
        }
        default: {
            // If we don't recognise the header, let the client fail, but don't
            // crash the planner
            SPDLOG_ERROR("Unrecognised sync call header: {}", header);
            return std::make_unique<faabric::EmptyResponse>();
        }
    }
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvPing()
{
    // Pong
    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message>
PlannerServer::recvGetAvailableHosts()
{
    auto response = std::make_unique<AvailableHostsResponse>();

    auto availableHosts = getPlanner().getAvailableHosts();

    for (auto host : availableHosts) {
        *response->add_hosts() = *host;
    }

    return std::move(response);
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvRegisterHost(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(RegisterHostRequest, buffer.data(), buffer.size());

    int hostId;
    bool success = getPlanner().registerHost(parsedMsg.host(), &hostId);
    if (!success) {
        SPDLOG_ERROR("Error registering host in Planner!");
    }

    auto response = std::make_unique<faabric::planner::RegisterHostResponse>();
    *response->mutable_config() = faabric::planner::getPlanner().getConfig();
    response->set_hostid(hostId);

    // Set response status (TODO: maybe make a macro of ths?)
    faabric::planner::ResponseStatus status;
    if (success) {
        status.set_status(faabric::planner::ResponseStatus_Status_OK);
    } else {
        status.set_status(faabric::planner::ResponseStatus_Status_ERROR);
    }
    *response->mutable_status() = status;

    return response;
}

void PlannerServer::recvRemoveHost(std::span<const uint8_t> buffer)
{
    PARSE_MSG(RemoveHostRequest, buffer.data(), buffer.size());

    // Removing a host is a best-effort operation, we just try to remove it if
    // we find it
    getPlanner().removeHost(parsedMsg.host());
}
}
