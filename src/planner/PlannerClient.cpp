#include <faabric/planner/PlannerApi.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/common.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>

namespace faabric::planner {
void KeepAliveThread::doWork()
{
    PlannerClient cli;

    faabric::util::SharedLock lock(keepAliveThreadMx);

    cli.registerHost(thisHostReq);
}

void KeepAliveThread::setRequest(
  std::shared_ptr<RegisterHostRequest> thisHostReqIn)
{
    faabric::util::FullLock lock(keepAliveThreadMx);

    thisHostReq = thisHostReqIn;
}

PlannerClient::PlannerClient()
  : faabric::transport::MessageEndpointClient(
      faabric::util::getIPFromHostname(
        faabric::util::getSystemConfig().plannerHost),
      PLANNER_ASYNC_PORT,
      PLANNER_SYNC_PORT)
{}

void PlannerClient::ping()
{
    EmptyRequest req;
    PingResponse resp;
    syncSend(PlannerCalls::Ping, &req, &resp);

    // Sanity check
    auto expectedIp = faabric::util::getIPFromHostname(
      faabric::util::getSystemConfig().plannerHost);
    auto gotIp = resp.config().ip();
    if (expectedIp != gotIp) {
        SPDLOG_ERROR(
          "Error pinging the planner server (expected ip: {} - got {})",
          expectedIp,
          gotIp);
        throw std::runtime_error("Error pinging the planner server");
    }

    SPDLOG_DEBUG("Succesfully pinged the planner server at {}", expectedIp);
}

void PlannerClient::setTestsConfig(PlannerTestsConfig& testsConfig)
{
    EmptyResponse resp;
    syncSend(PlannerCalls::SetTestsConfig, &testsConfig, &resp);
}

std::vector<faabric::planner::Host> PlannerClient::getAvailableHosts()
{
    EmptyRequest req;
    AvailableHostsResponse resp;
    syncSend(PlannerCalls::GetAvailableHosts, &req, &resp);

    // Copy the repeated array into a more convinient container
    std::vector<Host> availableHosts;
    for (int i = 0; i < resp.hosts_size(); i++) {
        availableHosts.push_back(resp.hosts(i));
    }

    return availableHosts;
}

int PlannerClient::registerHost(std::shared_ptr<RegisterHostRequest> req)
{
    RegisterHostResponse resp;
    syncSend(PlannerCalls::RegisterHost, req.get(), &resp);

    if (resp.status().status() != ResponseStatus_Status_OK) {
        throw std::runtime_error("Error registering host with planner!");
    }

    // Sanity check
    assert(resp.config().hosttimeout() > 0);

    // Return the planner's timeout to set the keep-alive heartbeat
    return resp.config().hosttimeout();
}

void PlannerClient::removeHost(std::shared_ptr<RemoveHostRequest> req)
{
    faabric::EmptyResponse response;
    syncSend(PlannerCalls::RemoveHost, req.get(), &response);
}

faabric::util::SchedulingDecision PlannerClient::callFunctions(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::PointToPointMappings response;
    syncSend(PlannerCalls::CallFunctions, req.get(), &response);

    return faabric::util::SchedulingDecision::fromPointToPointMappings(
      response);
}

faabric::util::SchedulingDecision PlannerClient::getSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::PointToPointMappings response;
    syncSend(PlannerCalls::GetSchedulingDecision, req.get(), &response);

    return faabric::util::SchedulingDecision::fromPointToPointMappings(
      response);
}

void PlannerClient::setMessageResult(std::shared_ptr<faabric::Message> msg)
{
    faabric::EmptyResponse response;
    syncSend(PlannerCalls::SetMessageResult, msg.get(), &response);
}

std::shared_ptr<faabric::Message> PlannerClient::getMessageResult(
  std::shared_ptr<faabric::Message> msg)
{
    faabric::Message responseMsg;
    syncSend(PlannerCalls::GetMessageResult, msg.get(), &responseMsg);

    if (responseMsg.id() == 0 && responseMsg.appid() == 0) {
        return nullptr;
    }

    return std::make_shared<faabric::Message>(responseMsg);
}

std::shared_ptr<faabric::BatchExecuteRequest> PlannerClient::getBatchMessages(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::BatchExecuteRequest responseReq;
    syncSend(PlannerCalls::GetBatchMessages, req.get(), &responseReq);

    if (responseReq.id() == 0 || responseReq.appid() == 0) {
        return nullptr;
    }

    return std::make_shared<faabric::BatchExecuteRequest>(responseReq);
}
}
