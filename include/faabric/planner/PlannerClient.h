#pragma once

#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::planner {

class PlannerClient final : public faabric::transport::MessageEndpointClient
{
  public:
    PlannerClient();

    void ping();

    void getAvailableHosts();

    void registerHost();

    void removeHost();
};
}
