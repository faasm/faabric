#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/planner/Planner.h>
#include <faabric/planner/PlannerEndpointHandler.h>
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

    // TODO: move this to src/util/json.cpp
    faabric::planner::HttpMessage msg;
    try {
        faabric::util::jsonToMessagePb(requestStr, &msg);
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
            bool success = faabric::planner::getPlanner().flush(faabric::planner::FlushType::Hosts);
            if (success) {
                response.result(beast::http::status::ok);
                response.body() = std::string("Flushed available hosts!");
            } else {
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("Failed flushing available hosts!");
            }
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_GET_CONFIG: {
            auto config = faabric::planner::getPlanner().getConfig();
            std::string responseStr;
            try {
                faabric::util::messageToJsonPb(config, &responseStr);
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

void PlannerEndpointHandler::executeFunction(
  faabric::endpoint::HttpRequestContext&& ctx,
  faabric::util::BeastHttpResponse&& response,
  std::shared_ptr<faabric::BatchExecuteRequest> ber,
  size_t messageIndex)
{
    /*
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    faabric::Message& msg = *ber->mutable_messages(messageIndex);

    if (msg.user().empty()) {
        response.result(beast::http::status::bad_request);
        response.body() = std::string("Empty user");
        return ctx.sendFunction(std::move(response));
    }

    if (msg.function().empty()) {
        response.result(beast::http::status::bad_request);
        response.body() = std::string("Empty function");
        return ctx.sendFunction(std::move(response));
    }

    // Set message ID and master host
    faabric::util::setMessageId(msg);
    std::string thisHost = faabric::util::getSystemConfig().endpointHost;
    msg.set_masterhost(thisHost);
    // This is set to false by the scheduler if the function ends up being sent
    // elsewhere
    if (!msg.isasync()) {
        msg.set_executeslocally(true);
    }

    auto tid = gettid();
    const std::string funcStr = faabric::util::funcToString(msg, true);
    SPDLOG_DEBUG("Worker HTTP thread {} scheduling {}", tid, funcStr);

    // Schedule it
    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.callFunctions(ber);

    // Await result on global bus (may have been executed on a different worker)
    if (msg.isasync()) {
        response.result(beast::http::status::ok);
        response.body() = faabric::util::buildAsyncResponse(msg);
        return ctx.sendFunction(std::move(response));
    }

    SPDLOG_DEBUG("Worker thread {} awaiting {}", tid, funcStr);
    sch.getFunctionResultAsync(
      msg.id(),
      conf.globalMessageTimeout,
      ctx.ioc,
      ctx.executor,
      beast::bind_front_handler(&PlannerEndpointHandler::onFunctionResult,
                                this->shared_from_this(),
                                std::move(ctx),
                                std::move(response)));
    */
}

void PlannerEndpointHandler::onFunctionResult(
  faabric::endpoint::HttpRequestContext&& ctx,
  faabric::util::BeastHttpResponse&& response,
  faabric::Message& result)
{
    /*
    beast::http::status statusCode =
      (result.returnvalue() == 0) ? beast::http::status::ok
                                  : beast::http::status::internal_server_error;
    response.result(statusCode);
    SPDLOG_DEBUG("Worker thread {} result {}",
                 gettid(),
                 faabric::util::funcToString(result, true));

    response.body() = result.outputdata();
    return ctx.sendFunction(std::move(response));
    */
}

}
