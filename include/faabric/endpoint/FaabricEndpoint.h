#pragma once

#include <pistache/endpoint.h>
#include <pistache/http.h>

#include <faabric/util/config.h>

namespace faabric::endpoint {

enum EndpointMode
{
    SIGNAL,
    BG_THREAD
};

class FaabricEndpoint
{
  public:
    FaabricEndpoint();

    FaabricEndpoint(int portIn, int threadCountIn);

    void start(EndpointMode mode);

    void stop();

  private:
    int port = faabric::util::getSystemConfig().endpointPort;
    int threadCount = faabric::util::getSystemConfig().endpointNumThreads;

    Pistache::Http::Endpoint httpEndpoint;

    std::jthread bgThread;
    std::mutex mx;
};
}
