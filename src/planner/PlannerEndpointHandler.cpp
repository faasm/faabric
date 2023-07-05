#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/planner/Planner.h>
#include <faabric/planner/PlannerEndpointHandler.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/ExecGraph.h>
#include <faabric/util/batch.h>
#include <faabric/util/json.h>
#include <faabric/util/logging.h>

namespace faabric::planner {

using header = beast::http::field;

// TODO(schedule): this atomic variable is used to temporarily select which
// host to forward an execute request to. This is because the planner still
// does not schedule resources to hosts, just acts as a proxy.
static std::atomic<int> nextHostIdx = 0;

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
            SPDLOG_DEBUG("Planner received RESET request");
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
            SPDLOG_DEBUG("Planner received FLUSH_HOSTS request");
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
            SPDLOG_DEBUG("Planner received FLUSH_EXECUTORS request");
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
            SPDLOG_DEBUG("Planner received GET_CONFIG request");
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
        case faabric::planner::HttpMessage_Type_GET_EXEC_GRAPH: {
            SPDLOG_DEBUG("Planner received GET_EXEC_GRAPH request");
            faabric::Message payloadMsg;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &payloadMsg);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in request body");
                return ctx.sendFunction(std::move(response));
            }

            auto execGraph = faabric::util::getFunctionExecGraph(payloadMsg);
            // An empty exec graph has one node with all fields null-ed
            if (execGraph.rootNode.msg.id() == 0) {
                SPDLOG_ERROR("Error processing GET_EXEC_GRAPH request");
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("Failed getting exec. graph!");
            } else {
                response.result(beast::http::status::ok);
                response.body() = faabric::util::execGraphToJson(execGraph);
            }
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_EXECUTE_BATCH: {
            // in: BatchExecuteRequest
            // out: BatchExecuteRequestStatus
            // Parse the message payload
            SPDLOG_DEBUG("Planner received EXECUTE_BATCH request");
            faabric::BatchExecuteRequest rawBer;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &rawBer);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in request body");
                return ctx.sendFunction(std::move(response));
            }
            auto ber = std::make_shared<faabric::BatchExecuteRequest>(rawBer);

            // Sanity check the BER
            if (!faabric::util::isBatchExecRequestValid(ber)) {
                response.result(beast::http::status::bad_request);
                response.body() = "Bad BatchExecRequest";
                return ctx.sendFunction(std::move(response));
            }
            ber->set_comesfromplanner(true);

            // Schedule and execute the BER
            // TODO: make scheduling decision here
            // FIXME: for the moment, just forward randomly to one node. Note
            // that choosing the node randomly may yield to uneven load
            // distributions
            auto availableHosts =
              faabric::planner::getPlanner().getAvailableHosts();
            if (availableHosts.empty()) {
                SPDLOG_ERROR("Planner doesn't have any registered hosts to"
                             " schedule EXECUTE_BATCH request to!");
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("No available hosts");
                return ctx.sendFunction(std::move(response));
            }
            // Note that hostIdx++ is an atomic increment
            int hostIdx = nextHostIdx++ % availableHosts.size();
            faabric::scheduler::getScheduler()
              .getFunctionCallClient(availableHosts.at(hostIdx)->ip())
              ->executeFunctions(ber);

            // Prepare the response
            response.result(beast::http::status::ok);
            auto berStatus = faabric::util::batchExecStatusFactory(ber);
            response.body() = faabric::util::messageToJson(*berStatus);

            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_EXECUTE_BATCH_STATUS: {
            // in: BatchExecuteRequestStatus
            // out: BatchExecuteRequestStatus
            // Parse the message payload
            SPDLOG_DEBUG("Planner received EXECUTE_BATCH_STATUS request");
            faabric::BatchExecuteRequestStatus berStatus;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &berStatus);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in request body");
                return ctx.sendFunction(std::move(response));
            }

            // Work-out how many message results we have for the requested BER
            auto actualBerStatus =
              faabric::planner::getPlanner().getBatchResults(berStatus.appid());

            // If the result is null, it means that the app id is not
            // registered in the results map. This is an error
            if (actualBerStatus == nullptr) {
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("App not registered in results");
                return ctx.sendFunction(std::move(response));
            }

            // Prepare the response
            response.result(beast::http::status::ok);
            // Work-out if it has finished using user-provided flags
            if (actualBerStatus->messageresults_size() ==
                berStatus.expectednummessages()) {
                actualBerStatus->set_finished(true);
            } else {
                actualBerStatus->set_finished(false);
            }
            response.body() = faabric::util::messageToJson(*actualBerStatus);

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
