#pragma once

#include <faabric/planner/planner.pb.h>
#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::planner {

class PlannerClient final : public faabric::transport::MessageEndpointClient
{
  public:
    PlannerClient();

    void ping();

    std::vector<faabric::planner::Host> getAvailableHosts();

    // Register this host with the planner. Returns the keep-alive timeout
    // (i.e. how often do we need to send a keep-alive heartbeat) and our
    // unique host id to identify further requests (as redundancy over our IP)
    std::pair<int, int> registerHost(std::shared_ptr<faabric::planner::RegisterHostRequest> req);

    void removeHost();
};
}
