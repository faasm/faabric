#pragma once

#include <pistache/endpoint.h>
#include <pistache/http.h>

#include <faabric/util/config.h>

namespace faabric::endpoint {
class FaabricEndpoint
{
  public:
    FaabricEndpoint();

    FaabricEndpoint(int portIn, int threadCountIn);

    void start(bool awaitSignal = true);

    void stop();

    std::shared_ptr<Pistache::Http::Handler> getHandler();

  private:
    int port = faabric::util::getSystemConfig().endpointPort;
    int threadCount = faabric::util::getSystemConfig().endpointNumThreads;

    Pistache::Http::Endpoint httpEndpoint;
};
}
