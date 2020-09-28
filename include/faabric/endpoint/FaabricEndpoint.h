#pragma once

#include <faabric/endpoint/Endpoint.h>
#include <faabric/util/config.h>

namespace faabric::endpoint {
class FaabricEndpoint : public Endpoint
{
  public:
    FaabricEndpoint();

    FaabricEndpoint(int port, int threadCount);

    std::shared_ptr<Pistache::Http::Handler> getHandler() override;
};
}
