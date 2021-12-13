#include <faabric/endpoint/FaabricEndpointHandler.h>

#include <faabric/redis/Redis.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/json.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#include <syscall.h>

namespace faabric::endpoint {
void FaabricEndpointHandler::onTimeout(const Pistache::Http::Request& request,
                                       Pistache::Http::ResponseWriter writer)
{
    writer.send(Pistache::Http::Code::No_Content);
}

void FaabricEndpointHandler::onRequest(const Pistache::Http::Request& request,
                                       Pistache::Http::ResponseWriter response)
{
    SPDLOG_DEBUG("Faabric handler received request");

    // Very permissive CORS
    response.headers().add<Pistache::Http::Header::AccessControlAllowOrigin>(
      "*");
    response.headers().add<Pistache::Http::Header::AccessControlAllowMethods>(
      "GET,POST,PUT,OPTIONS");
    response.headers().add<Pistache::Http::Header::AccessControlAllowHeaders>(
      "User-Agent,Content-Type");

    // Text response type
    response.headers().add<Pistache::Http::Header::ContentType>(
      Pistache::Http::Mime::MediaType("text/plain"));

    PROF_START(endpointRoundTrip)

    // Set response timeout
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    response.timeoutAfter(std::chrono::milliseconds(conf.globalMessageTimeout));

    // Parse message from JSON in request
    const std::string requestStr = request.body();
    std::pair<int, std::string> result = handleFunction(requestStr);

    PROF_END(endpointRoundTrip)
    Pistache::Http::Code responseCode = Pistache::Http::Code::Ok;
    if (result.first > 0) {
        responseCode = Pistache::Http::Code::Internal_Server_Error;
    }
    response.send(responseCode, result.second);
}

std::pair<int, std::string> FaabricEndpointHandler::handleFunction(
  const std::string& requestStr)
{
    std::pair<int, std::string> response;
    if (requestStr.empty()) {
        SPDLOG_ERROR("Faabric handler received empty request");
        response = std::make_pair(1, "Empty request");
    } else {
        faabric::Message msg = faabric::util::jsonToMessage(requestStr);
        faabric::scheduler::Scheduler& sched =
          faabric::scheduler::getScheduler();

        if (msg.isstatusrequest()) {
            SPDLOG_DEBUG("Processing status request");
            const faabric::Message result =
              sched.getFunctionResult(msg.id(), 0);

            if (result.type() == faabric::Message_MessageType_EMPTY) {
                response = std::make_pair(0, "RUNNING");
            } else if (result.returnvalue() == 0) {
                std::string responseStr =
                  fmt::format("SUCCESS: {}\nExecuted time (ms): {}",
                              result.outputdata(),
                              result.finishtimestamp() - result.timestamp());
                response = std::make_pair(0, responseStr);
            } else {
                response = std::make_pair(1, "FAILED: " + result.outputdata());
            }
        } else if (msg.isexecgraphrequest()) {
            SPDLOG_DEBUG("Processing execution graph request");
            faabric::scheduler::ExecGraph execGraph =
              sched.getFunctionExecGraph(msg.id());
            response =
              std::make_pair(0, faabric::scheduler::execGraphToJson(execGraph));

        } else if (msg.type() == faabric::Message_MessageType_FLUSH) {
            SPDLOG_DEBUG("Broadcasting flush request");
            sched.broadcastFlush();
            response = std::make_pair(0, "Flush sent");
        } else {
            response = executeFunction(msg);
        }
    }

    return response;
}

std::pair<int, std::string> FaabricEndpointHandler::executeFunction(
  faabric::Message& msg)
{
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();

    if (msg.user().empty()) {
        return std::make_pair(1, "Empty user");
    }

    if (msg.function().empty()) {
        return std::make_pair(1, "Empty function");
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

    auto tid = (pid_t)syscall(SYS_gettid);
    const std::string funcStr = faabric::util::funcToString(msg, true);
    SPDLOG_DEBUG("Worker HTTP thread {} scheduling {}", tid, funcStr);

    // Schedule it
    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.callFunction(msg);

    // Await result on global bus (may have been executed on a different worker)
    if (msg.isasync()) {
        return std::make_pair(0, faabric::util::buildAsyncResponse(msg));
    }

    SPDLOG_DEBUG("Worker thread {} awaiting {}", tid, funcStr);

    try {
        const faabric::Message result =
          sch.getFunctionResult(msg.id(), conf.globalMessageTimeout);
        SPDLOG_DEBUG("Worker thread {} result {}", tid, funcStr);

        if (result.sgxresult().empty()) {
            return std::make_pair(result.returnvalue(),
                                  result.outputdata() + "\n");
        }

        return std::make_pair(result.returnvalue(),
                              faabric::util::getJsonOutput(result));
    } catch (faabric::redis::RedisNoResponseException& ex) {
        return std::make_pair(1, "No response from function\n");
    }
}
}
