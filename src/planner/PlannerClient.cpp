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
}

void PlannerClient::getAvailableHosts()
{
    ;
}

void PlannerClient::registerHost()
{
    ;
}

void PlannerClient::removeHost()
{
    ;
}
}
