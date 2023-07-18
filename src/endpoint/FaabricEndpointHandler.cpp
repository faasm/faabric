#include <faabric/endpoint/FaabricEndpointHandler.h>

#include <faabric/redis/Redis.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/batch.h>
#include <faabric/util/json.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#include <syscall.h>

namespace faabric::endpoint {

using header = beast::http::field;

void FaabricEndpointHandler::onRequest(
  HttpRequestContext&& ctx,
  faabric::util::BeastHttpRequest&& request)
{
    SPDLOG_TRACE("Faabric handler received request");

    // Very permissive CORS
    faabric::util::BeastHttpResponse response;
    response.keep_alive(request.keep_alive());
    response.set(header::server, "Faabric endpoint");
    response.set(header::access_control_allow_origin, "*");
    response.set(header::access_control_allow_methods, "GET,POST,PUT,OPTIONS");
    response.set(header::access_control_allow_headers,
                 "User-Agent,Content-Type");

    // Text response type
    response.set(header::content_type, "text/plain");

    // TODO: for the moment we keep the endpoint handler, but we are not meant
    // to receive any requests here. Eventually we will delete it
    SPDLOG_ERROR("Faabric handler received empty request");
    response.result(beast::http::status::bad_request);
    response.body() = std::string("Empty request");
    ctx.sendFunction(std::move(response));
}
}
