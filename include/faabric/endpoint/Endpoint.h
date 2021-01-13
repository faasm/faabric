#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/config.h>
#include <pistache/endpoint.h>
#include <pistache/http.h>

#include <thread>

namespace faabric::endpoint {
class Endpoint
{
  public:
    Endpoint() = default;

    Endpoint(int port, int threadCount);

    void start(bool background = false);

    void doStart();

    virtual std::shared_ptr<Pistache::Http::Handler> getHandler() = 0;

  private:
    int port = faabric::util::getSystemConfig().endpointPort;
    int threadCount = faabric::util::getSystemConfig().endpointNumThreads;
    std::thread servingThread;
};
}
