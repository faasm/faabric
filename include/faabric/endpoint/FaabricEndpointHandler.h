#pragma once

#include <faabric/proto/faabric.pb.h>
#include <pistache/http.h>

namespace faabric::endpoint {
class FaabricEndpointHandler : public Pistache::Http::Handler
{
  public:
    HTTP_PROTOTYPE(FaabricEndpointHandler)

    void onTimeout(const Pistache::Http::Request& request,
                   Pistache::Http::ResponseWriter writer) override;

    void onRequest(const Pistache::Http::Request& request,
                   Pistache::Http::ResponseWriter response) override;

    std::string handleFunction(const std::string& requestStr);

  private:
    std::string executeFunction(faabric::Message& msg);
};
}
