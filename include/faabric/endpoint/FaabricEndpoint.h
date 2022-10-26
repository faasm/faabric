#pragma once

#include <functional>
#include <memory>

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/asio.h>
#include <faabric/util/config.h>

namespace faabric::endpoint {

enum class EndpointMode
{
    SIGNAL,
    BG_THREAD
};

namespace detail {
class EndpointState;
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

class FaabricEndpoint
{
  public:
    FaabricEndpoint();

    FaabricEndpoint(
      int port,
      int threadCount,
      std::shared_ptr<HttpRequestHandler> requestHandlerIn = nullptr);

    FaabricEndpoint(const FaabricEndpoint&) = delete;

    FaabricEndpoint(FaabricEndpoint&&) = delete;

    FaabricEndpoint& operator=(const FaabricEndpoint&) = delete;

    FaabricEndpoint& operator=(FaabricEndpoint&&) = delete;

    virtual ~FaabricEndpoint();

    void start(EndpointMode mode = EndpointMode::SIGNAL);

    void stop();

  private:
    int port;
    int threadCount;
    std::unique_ptr<detail::EndpointState> state;
    std::shared_ptr<HttpRequestHandler> requestHandler;
};
}
