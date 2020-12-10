#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/endpoint/FaabricEndpointHandler.h>

namespace faabric::endpoint {
FaabricEndpoint::FaabricEndpoint()
  : Endpoint()
{}

FaabricEndpoint::FaabricEndpoint(int port, int threadCount)
  : Endpoint(port, threadCount)
{}

std::shared_ptr<Pistache::Http::Handler> FaabricEndpoint::getHandler()
{
    return Pistache::Http::make_handler<FaabricEndpointHandler>();
}

}
