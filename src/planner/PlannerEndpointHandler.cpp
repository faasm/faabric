#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/planner/Planner.h>
#include <faabric/planner/PlannerEndpointHandler.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/util/json.h>
#include <faabric/util/logging.h>

namespace faabric::planner {

using header = beast::http::field;

void PlannerEndpointHandler::onRequest(
  faabric::endpoint::HttpRequestContext&& ctx,
  faabric::util::BeastHttpRequest&& request)
{
    SPDLOG_DEBUG("Faabric planner received request");

    // Very permissive CORS
    faabric::util::BeastHttpResponse response;
    response.keep_alive(request.keep_alive());
    response.set(header::server, "Planner endpoint");
    response.set(header::access_control_allow_origin, "*");
    response.set(header::access_control_allow_methods, "GET,POST,PUT,OPTIONS");
    response.set(header::access_control_allow_headers,
                 "User-Agent,Content-Type");

    // Text response type
    response.set(header::content_type, "text/plain");

    // Request body contains a string that is formatted as a JSON
    const std::string& requestStr = request.body();

    // Handle JSON
    if (requestStr.empty()) {
        SPDLOG_ERROR("Planner handler received empty request");
        response.result(beast::http::status::bad_request);
        response.body() = std::string("Empty request");
        return ctx.sendFunction(std::move(response));
    }

    faabric::planner::HttpMessage msg;
    try {
        faabric::util::jsonToMessage(requestStr, &msg);
    } catch (faabric::util::JsonSerialisationException e) {
        response.result(beast::http::status::bad_request);
        response.body() = std::string("Bad JSON in request body");
        return ctx.sendFunction(std::move(response));
    }

    switch (msg.type()) {
        case faabric::planner::HttpMessage_Type_RESET: {
            bool success = faabric::planner::getPlanner().reset();
            if (success) {
                response.result(beast::http::status::ok);
                response.body() = std::string("Planner fully reset!");
            } else {
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("Failed to reset planner");
            }
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_FLUSH_HOSTS: {
            bool success = faabric::planner::getPlanner().flush(
              faabric::planner::FlushType::Hosts);
            if (success) {
                response.result(beast::http::status::ok);
                response.body() = std::string("Flushed available hosts!");
            } else {
                response.result(beast::http::status::internal_server_error);
                response.body() =
                  std::string("Failed flushing available hosts!");
            }
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_FLUSH_EXECUTORS: {
            bool success = faabric::planner::getPlanner().flush(
              faabric::planner::FlushType::Executors);
            if (success) {
                response.result(beast::http::status::ok);
                response.body() = std::string("Flushed executors!");
            } else {
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("Failed flushing executors!");
            }
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_GET_CONFIG: {
            auto config = faabric::planner::getPlanner().getConfig();
            std::string responseStr;
            try {
                responseStr = faabric::util::messageToJson(config);
                response.result(beast::http::status::ok);
                response.body() = responseStr;
            } catch (faabric::util::JsonSerialisationException& e) {
                SPDLOG_ERROR("Error processing GET_CONFIG request");
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("Failed getting config!");
            }
            return ctx.sendFunction(std::move(response));
        }
        default: {
            SPDLOG_ERROR("Unrecognised message type {}", msg.type());
            response.result(beast::http::status::bad_request);
            response.body() = std::string("Unrecognised message type");
            return ctx.sendFunction(std::move(response));
        }
    }
}
}
