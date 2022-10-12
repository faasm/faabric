#pragma once

#include <faabric/endpoint/FaabricEndpoint.h>
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
                         std::shared_ptr<faabric::BatchExecuteRequest> ber,
                         size_t messageIndex);

    void onFunctionResult(HttpRequestContext&& ctx,
                          faabric::util::BeastHttpResponse&& partialResponse,
                          faabric::Message& msg);
};
}
