#pragma once

#include <faabric/endpoint/Endpoint.h>
#include <faabric/proto/faabric.pb.h>

namespace faabric::endpoint {
class FaabricEndpointHandler final
  : public HttpRequestHandler
  , public std::enable_shared_from_this<FaabricEndpointHandler>
{
  public:
    void onRequest(HttpRequestContext&& ctx,
                   faabric::util::BeastHttpRequest&& request) override;

  private:
    void executeFunction(HttpRequestContext&& ctx,
                         faabric::util::BeastHttpResponse&& partialResponse,
                         faabric::Message&& msg);

    void onFunctionResult(HttpRequestContext&& ctx,
                          faabric::util::BeastHttpResponse&& partialResponse,
                          faabric::Message& msg);
};
}
