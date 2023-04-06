#include <faabric/planner/planner.pb.h>
#include <faabric/planner/PlannerApi.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/transport/common.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>

namespace faabric::planner {

PlannerClient::PlannerClient()
  : faabric::transport::MessageEndpointClient(faabric::util::getIPFromHostname(faabric::util::getSystemConfig().plannerHost),
                                              PLANNER_ASYNC_PORT,
                                              PLANNER_SYNC_PORT)
{}

void PlannerClient::ping()
{
    faabric::planner::EmptyRequest req;
    faabric::planner::EmptyResponse resp;
    syncSend(faabric::planner::PlannerCalls::Ping, &req, &resp);
}

std::vector<faabric::planner::Host> PlannerClient::getAvailableHosts()
{
    faabric::planner::EmptyRequest req;
    faabric::planner::AvailableHostsResponse resp;
    syncSend(faabric::planner::PlannerCalls::GetAvailableHosts, &req, &resp);

    // Copy the repeated array into a more convinient container
    std::vector<faabric::planner::Host> availableHosts;
    for (int i = 0; i < resp.hosts_size(); i++) {
        availableHosts.push_back(resp.hosts(i));
    }

    return availableHosts;
}

std::pair<int, int> PlannerClient::registerHost(
  std::shared_ptr<faabric::planner::RegisterHostRequest> req)
{
    faabric::planner::RegisterHostResponse resp;
    syncSend(faabric::planner::PlannerCalls::RegisterHost, req.get(), &resp);

    if (resp.status().status() != faabric::planner::ResponseStatus_Status_OK) {
        throw std::runtime_error("Error registering host with planner!");
    }

    assert(resp.config().hosttimeout() > 0);
    assert(resp.hostid() > 0);

    return std::make_pair<int, int>(resp.config().hosttimeout(), resp.hostid());
}

void PlannerClient::removeHost()
{
    ;
}
}
