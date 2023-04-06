#pragma once

#include <faabric/endpoint/FaabricEndpoint.h>

namespace faabric::planner {
class PlannerEndpointHandler final
  : public faabric::endpoint::HttpRequestHandler
  , public std::enable_shared_from_this<PlannerEndpointHandler>
{
  public:
    void onRequest(faabric::endpoint::HttpRequestContext&& ctx,
                   faabric::util::BeastHttpRequest&& request) override;
};
}
