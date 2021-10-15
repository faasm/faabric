#pragma once

#include <functional>
#include <memory>

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/asio.h>
#include <faabric/util/config.h>

namespace faabric::endpoint {
namespace detail {
struct EndpointState;
}

struct HttpRequestContext
{
    asio::io_context& ioc;
    asio::any_io_executor executor;
    std::function<void(faabric::util::BeastHttpResponse&&)> sendFunction;
};

class HttpRequestHandler
{
  public:
    virtual void onRequest(HttpRequestContext&& ctx,
                           faabric::util::BeastHttpRequest&& request) = 0;
};

class Endpoint
{
  public:
    Endpoint() = delete;
    Endpoint(const Endpoint&) = delete;
    Endpoint(Endpoint&&) = delete;
    Endpoint& operator=(const Endpoint&) = delete;
    Endpoint& operator=(Endpoint&&) = delete;
    virtual ~Endpoint();

    Endpoint(int port,
             int threadCount,
             std::shared_ptr<HttpRequestHandler> requestHandlerIn);

    void start(bool awaitSignal = true);

    void stop();

  private:
    int port;
    int threadCount;
    std::unique_ptr<detail::EndpointState> state;
    std::shared_ptr<HttpRequestHandler> requestHandler;
};
}
