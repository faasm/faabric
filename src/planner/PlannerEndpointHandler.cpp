#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/planner/Planner.h>
#include <faabric/planner/PlannerEndpointHandler.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/util/func.h>
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
        case faabric::planner::HttpMessage_Type_GET_APP_MESSAGES: {
            faabric::BatchExecuteRequest req;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &req);
            } catch (faabric::util::JsonSerialisationException& e) {
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("Error deserialising GET_APP_MESSAGES request");
            }

            auto resultReq = faabric::planner::getPlanner().getBatchMessages(
              std::make_shared<faabric::BatchExecuteRequest>(req));

            response.result(beast::http::status::ok);
            response.body() = faabric::util::messageToJson(*resultReq);
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_GET_AVAILABLE_HOSTS: {
            auto availableHosts = faabric::planner::getPlanner().getAvailableHosts();

            faabric::planner::GetAvailableHostsResponse hostsResponse;
            for (auto& host : availableHosts) {
                *hostsResponse.add_hosts() = *host;
            }

            response.result(beast::http::status::ok);
            response.body() = faabric::util::messageToJson(hostsResponse);
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_GET_IN_FLIGHT_APPS: {
            auto inFlightApps = faabric::planner::getPlanner().getInFlightBatches();

            faabric::planner::GetInFlightAppsResponse inFlightAppsResponse;
            for (const auto& app : inFlightApps) {
                auto* inFlightApp = inFlightAppsResponse.add_apps();
                inFlightApp->set_appid(app->appid());
                for (const auto& msg : app->messages()) {
                    inFlightApp->add_hostips(msg.executedhost());
                }
            }

            response.result(beast::http::status::ok);
            response.body() = faabric::util::messageToJson(inFlightAppsResponse);
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_FLUSH_AVAILABLE_HOSTS: {
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
        case faabric::planner::HttpMessage_Type_EXECUTE: {
            faabric::Message funcMsg;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &funcMsg);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() =
                  std::string("Wrong payload for execute function");
                return ctx.sendFunction(std::move(response));
            }

            // Sanity check input message
            if (funcMsg.user().empty()) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Empty user");
                return ctx.sendFunction(std::move(response));
            }
            if (funcMsg.function().empty()) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Empty function");
                return ctx.sendFunction(std::move(response));
            }

            // Set request
            auto req = faabric::util::batchExecFactory(
              funcMsg.user(), funcMsg.function(), 0);
            req->set_type(req->FUNCTIONS);
            funcMsg.set_appid(req->appid());
            faabric::util::setMessageId(funcMsg);
            *req->add_messages() = funcMsg;

            const std::string funcStr =
              faabric::util::funcToString(funcMsg, true);
            SPDLOG_DEBUG("Worker HTTP thread scheduling {}", funcStr);

            // Make scheduling decision for message
            auto& planner = getPlanner();
            auto schedulingDecision = planner.makeSchedulingDecision(req);
            if (!schedulingDecision) {
                response.result(beast::http::status::internal_server_error);
                response.body() = "Error making scheduling decision for app";
                return ctx.sendFunction(std::move(response));
            }

            // Print the scheduling decision
            schedulingDecision->print();

            // Dispatch to the corresponding workers
            planner.dispatchSchedulingDecision(req, schedulingDecision);

            // TODO: Remove async altogether
            response.result(beast::http::status::ok);
            // response.body() = faabric::util::buildAsyncResponse(funcMsg);
            response.body() = faabric::util::messageToJson(funcMsg);
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_EXECUTE_STATUS: {
            // Get message from request string
            faabric::Message funcMsg;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &funcMsg);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string(
                  "Wrong payload for get function execution status");
                return ctx.sendFunction(std::move(response));
            }

            // As a sanity-check, make sure the message's master host is unset,
            // this will prevent the planner from trying to notify us when
            // the message is ready
            funcMsg.clear_masterhost();

            // Get actual message result, and populate HTTP response
            auto resultMsg = getPlanner().getMessageResult(
              std::make_shared<faabric::Message>(funcMsg));
            if (!resultMsg) {
                response.result(beast::http::status::ok);
                response.body() = std::string("RUNNING");
            } else if (resultMsg->returnvalue() == 0) {
                response.result(beast::http::status::ok);
                response.body() = faabric::util::messageToJson(*resultMsg);
            } else {
                response.result(beast::http::status::internal_server_error);
                response.body() = "FAILED: " + resultMsg->outputdata();
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
