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
      getPlanner().getConfig().numthreadshttpserver())
  , planner(getPlanner())
{}

void PlannerServer::doAsyncRecv(transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    switch (header) {
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
        case PlannerCalls::Ping: {
            return recvPing();
        }
        case PlannerCalls::SetTestsConfig: {
            return recvSetTestsConfig(message.udata());
        }
        case PlannerCalls::GetAvailableHosts: {
            return recvGetAvailableHosts();
        }
        case PlannerCalls::RegisterHost: {
            return recvRegisterHost(message.udata());
        }
        case PlannerCalls::RemoveHost: {
            return recvRemoveHost(message.udata());
        }
        case PlannerCalls::CallFunctions: {
            return recvCallFunctions(message.udata());
        }
        case PlannerCalls::GetSchedulingDecision: {
            return recvGetSchedulingDecision(message.udata());
        }
        case PlannerCalls::SetMessageResult: {
            return recvSetMessageResult(message.udata());
        }
        case PlannerCalls::GetMessageResult: {
            return recvGetMessageResult(message.udata());
        }
        case PlannerCalls::GetBatchResult: {
            return recvGetBatchResult(message.udata());
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
    auto response = std::make_unique<faabric::planner::PingResponse>();
    *response->mutable_config() = planner.getConfig();

    return std::move(response);
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvSetTestsConfig(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(PlannerTestsConfig, buffer.data(), buffer.size());

    planner.setTestsConfig(parsedMsg);

    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message>
PlannerServer::recvGetAvailableHosts()
{
    auto response = std::make_unique<AvailableHostsResponse>();

    auto availableHosts = planner.getAvailableHosts();

    for (auto host : availableHosts) {
        *response->add_hosts() = *host;
    }

    return std::move(response);
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvRegisterHost(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(RegisterHostRequest, buffer.data(), buffer.size());

    bool success = planner.registerHost(parsedMsg.host());
    if (!success) {
        SPDLOG_ERROR("Error registering host in Planner!");
    }

    auto response = std::make_unique<faabric::planner::RegisterHostResponse>();
    *response->mutable_config() = planner.getConfig();

    // Set response status
    ResponseStatus status;
    if (success) {
        status.set_status(ResponseStatus_Status_OK);
    } else {
        status.set_status(ResponseStatus_Status_ERROR);
    }
    *response->mutable_status() = status;

    return response;
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvRemoveHost(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(RemoveHostRequest, buffer.data(), buffer.size());

    // Removing a host is a best-effort operation, we just try to remove it if
    // we find it
    planner.removeHost(parsedMsg.host());

    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvCallFunctions(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(BatchExecuteRequest, buffer.data(), buffer.size());

    auto req = std::make_shared<faabric::BatchExecuteRequest>(parsedMsg);
    bool isDistChange =
      req->type() == faabric::BatchExecuteRequest::DIST_CHANGE;

    // Note: we currently ignore the topology hint :/
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    faabric::util::SchedulingTopologyHint topologyHint =
      firstMsg.topologyhint().empty()
        ? faabric::util::SchedulingTopologyHint::NONE
        : faabric::util::strToTopologyHint.at(firstMsg.topologyhint());

    auto decision = planner.makeSchedulingDecision(req, topologyHint);
    // Catch failures when making a scheduling decision, and return an empty
    // PTP mapping
    if (decision == nullptr) {
        return std::make_unique<faabric::PointToPointMappings>();
    }
    decision->debugPrint();
    // If the request is a DIST_CHANGE (i.e. migration) we don't need to
    // dispatch new execution calls
    if (!isDistChange) {
        planner.dispatchSchedulingDecision(req, decision);
    }

    // Build PointToPointMappings from scheduling decision
    faabric::PointToPointMappings mappings;
    mappings.set_appid(decision->appId);
    mappings.set_groupid(decision->groupId);
    for (int i = 0; i < decision->hosts.size(); i++) {
        auto* mapping = mappings.add_mappings();
        mapping->set_host(decision->hosts.at(i));
        mapping->set_messageid(decision->messageIds.at(i));
        mapping->set_appidx(decision->appIdxs.at(i));
        mapping->set_groupidx(decision->groupIdxs.at(i));
    }

    return std::make_unique<faabric::PointToPointMappings>(mappings);
}

std::unique_ptr<google::protobuf::Message>
PlannerServer::recvGetSchedulingDecision(std::span<const uint8_t> buffer)
{
    PARSE_MSG(BatchExecuteRequest, buffer.data(), buffer.size());

    auto req = std::make_shared<faabric::BatchExecuteRequest>(parsedMsg);

    auto decision = planner.getSchedulingDecision(req);

    if (decision == nullptr) {
        return std::make_unique<faabric::PointToPointMappings>();
    }

    // Build PointToPointMappings from scheduling decision
    faabric::PointToPointMappings mappings;
    mappings.set_appid(decision->appId);
    mappings.set_groupid(decision->groupId);
    for (int i = 0; i < decision->hosts.size(); i++) {
        auto* mapping = mappings.add_mappings();
        mapping->set_host(decision->hosts.at(i));
        mapping->set_messageid(decision->messageIds.at(i));
        mapping->set_appidx(decision->appIdxs.at(i));
        mapping->set_groupidx(decision->groupIdxs.at(i));
    }

    return std::make_unique<faabric::PointToPointMappings>(mappings);
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvSetMessageResult(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(Message, buffer.data(), buffer.size());

    planner.setMessageResult(std::make_shared<faabric::Message>(parsedMsg));

    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvGetMessageResult(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(Message, buffer.data(), buffer.size());

    auto resultMsg =
      planner.getMessageResult(std::make_shared<faabric::Message>(parsedMsg));

    if (resultMsg == nullptr) {
        resultMsg = std::make_shared<faabric::Message>();
        resultMsg->set_appid(0);
        resultMsg->set_id(0);
    }

    return std::make_unique<faabric::Message>(*resultMsg);
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvGetBatchResult(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(BatchExecuteRequest, buffer.data(), buffer.size());

    auto resultReq =
      planner.getBatchResult(std::make_shared<faabric::BatchExecuteRequest>(parsedMsg));

    return std::make_unique<faabric::BatchExecuteRequest>(*resultReq);
}
}
