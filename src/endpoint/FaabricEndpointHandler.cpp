#include <faabric/endpoint/FaabricEndpointHandler.h>

#include <faabric/redis/Redis.h>
#include <faabric/scheduler/Scheduler.h>
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

    PROF_START(endpointRoundTrip)

    // Parse message from JSON in request
    const std::string& requestStr = request.body();

    // Handle JSON
    if (requestStr.empty()) {
        SPDLOG_ERROR("Faabric handler received empty request");
        response.result(beast::http::status::bad_request);
        response.body() = std::string("Empty request");
    } else {
        auto req = faabric::util::batchExecFactory();
        req->set_type(req->FUNCTIONS);
        faabric::Message& msg = *req->add_messages();
        faabric::util::jsonToMessage(requestStr, &msg);
        faabric::scheduler::Scheduler& sched =
          faabric::scheduler::getScheduler();

        if (msg.isstatusrequest()) {
            SPDLOG_DEBUG("Processing status request");
            const faabric::Message result = sched.getFunctionResult(msg, 0);

            if (result.type() == faabric::Message_MessageType_EMPTY) {
                response.result(beast::http::status::ok);
                response.body() = std::string("RUNNING");
            } else if (result.returnvalue() == 0) {
                response.result(beast::http::status::ok);
                response.body() = faabric::util::messageToJson(result);
            } else {
                response.result(beast::http::status::internal_server_error);
                response.body() = "FAILED: " + result.outputdata();
            }
        } else if (msg.isexecgraphrequest()) {
            SPDLOG_DEBUG("Processing execution graph request");
            faabric::scheduler::ExecGraph execGraph =
              sched.getFunctionExecGraph(msg);
            response.result(beast::http::status::ok);
            response.body() = faabric::scheduler::execGraphToJson(execGraph);
        } else {
            executeFunction(
              std::move(ctx), std::move(response), std::move(req), 0);
            return;
        }
    }

    PROF_END(endpointRoundTrip)
    ctx.sendFunction(std::move(response));
}

void FaabricEndpointHandler::executeFunction(
  HttpRequestContext&& ctx,
  faabric::util::BeastHttpResponse&& response,
  std::shared_ptr<faabric::BatchExecuteRequest> ber,
  size_t messageIndex)
{
    auto& conf = faabric::util::getSystemConfig();

    // Set app ID, message ID and master host on the first message of the BER
    // TODO: eventually do it on the BER itself
    faabric::util::setMessageId(*ber->mutable_messages(0));
    ber->mutable_messages(0)->set_masterhost(conf.endpointHost);
    int appId = ber->messages(0).appid();
    int msgId = ber->messages(0).id();
    assert(appId != 0);
    assert(msgId != 0);

    if (ber->messages(0).user().empty()) {
        response.result(beast::http::status::bad_request);
        response.body() = std::string("Empty user");
        return ctx.sendFunction(std::move(response));
    }

    if (ber->messages(0).function().empty()) {
        response.result(beast::http::status::bad_request);
        response.body() = std::string("Empty function");
        return ctx.sendFunction(std::move(response));
    }

    // This is set to false by the scheduler if the function ends up being sent
    // elsewhere
    if (!ber->messages(0).isasync()) {
        ber->mutable_messages(0)->set_executeslocally(true);
    }

    auto tid = gettid();
    const std::string funcStr =
      faabric::util::funcToString(ber->messages(0), true);
    SPDLOG_DEBUG("Worker HTTP thread {} scheduling {}", tid, funcStr);

    // Schedule it
    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.callFunctions(ber);

    // Await result on global bus (may have been executed on a different worker)
    if (ber->messages(0).isasync()) {
        response.result(beast::http::status::ok);
        response.body() = faabric::util::messageToJson(ber->messages(0));
        return ctx.sendFunction(std::move(response));
    }

    // TODO: temporarily make this HTTP call block one server thread.
    // Eventually. we will route all HTTP requests through the planner instead
    // of the worker, so we will be able to remove this blocking call
    SPDLOG_DEBUG("Worker thread {} awaiting {}", tid, funcStr);
    auto result =
      sch.getFunctionResult(appId, msgId, conf.globalMessageTimeout);

    beast::http::status statusCode =
      (result.returnvalue() == 0) ? beast::http::status::ok
                                  : beast::http::status::internal_server_error;
    response.result(statusCode);
    SPDLOG_DEBUG("Worker thread {} result {}",
                 gettid(),
                 faabric::util::funcToString(result, true));

    response.body() = result.outputdata();
    return ctx.sendFunction(std::move(response));
}
}
