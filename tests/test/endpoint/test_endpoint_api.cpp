#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/endpoint/FaabricEndpointHandler.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/json.h>
#include <faabric/util/macros.h>

using namespace Pistache;
using namespace faabric::scheduler;

namespace tests {

// This is a bit gnarly, we get "Address already in use" errors if we try to use
// the same port for each case, so we need to switch it every time.
static int port = 8080;

class EndpointApiTestExecutor final : public Executor
{
  public:
    EndpointApiTestExecutor(faabric::Message& msg)
      : Executor(msg)
    {}

    ~EndpointApiTestExecutor() {}

    int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> reqOrig) override
    {
        faabric::Message& msg = reqOrig->mutable_messages()->at(msgIdx);

        int returnVal = 0;
        if (msg.function() == "valid") {
            msg.set_outputdata(
              fmt::format("Endpoint API test executed {}", msg.id()));

        } else if (msg.function() == "error") {
            returnVal = 1;
            msg.set_outputdata(fmt::format(
              "Endpoint API returning {} for {}", returnVal, msg.id()));
        } else if (msg.isasync()) {
            returnVal = 0;
            SLEEP_MS(3000);
            msg.set_outputdata(
              fmt::format("Finished async message {}", msg.id()));
        } else {
            throw std::runtime_error("Endpoint API error");
        }

        return returnVal;
    }
};

class EndpointApiTestExecutorFactory : public ExecutorFactory
{
  protected:
    std::shared_ptr<Executor> createExecutor(faabric::Message& msg) override
    {
        return std::make_shared<EndpointApiTestExecutor>(msg);
    }
};

class EndpointApiTestFixture : public SchedulerTestFixture
{
  public:
    EndpointApiTestFixture()
    {
        executorFactory = std::make_shared<EndpointApiTestExecutorFactory>();
        setExecutorFactory(executorFactory);
    }

    ~EndpointApiTestFixture() = default;

  protected:
    std::shared_ptr<EndpointApiTestExecutorFactory> executorFactory;
};

TEST_CASE_METHOD(EndpointApiTestFixture,
                 "Test requests to endpoint",
                 "[endpoint]")
{
    port++;

    faabric::endpoint::FaabricEndpoint endpoint(port, 2);

    std::thread serverThread([&endpoint]() { endpoint.start(false); });

    // Wait for the server to start
    SLEEP_MS(2000);

    std::string body;
    int expectedReturnCode = 200;
    std::string expectedResponseBody;

    SECTION("Empty request")
    {
        expectedReturnCode = 500;
        expectedResponseBody = "Empty request";
    }

    SECTION("Valid request")
    {
        faabric::Message msg = faabric::util::messageFactory("foo", "valid");
        body = faabric::util::messageToJson(msg);
        expectedReturnCode = 200;
        expectedResponseBody =
          fmt::format("Endpoint API test executed {}\n", msg.id());
    }

    SECTION("Error request")
    {
        faabric::Message msg = faabric::util::messageFactory("foo", "error");
        body = faabric::util::messageToJson(msg);
        expectedReturnCode = 500;
        expectedResponseBody =
          fmt::format("Endpoint API returning 1 for {}\n", msg.id());
    }

    SECTION("Invalid function")
    {
        faabric::Message msg = faabric::util::messageFactory("foo", "junk");
        body = faabric::util::messageToJson(msg);
        expectedReturnCode = 500;
        expectedResponseBody = fmt::format(
          "Task {} threw exception. What: Endpoint API error\n", msg.id());
    }

    std::pair<int, std::string> result =
      submitGetRequestToUrl(LOCALHOST, port, body);
    REQUIRE(result.first == expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);

    endpoint.stop();

    if (serverThread.joinable()) {
        serverThread.join();
    }
}

TEST_CASE_METHOD(EndpointApiTestFixture,
                 "Test status requests to endpoint",
                 "[endpoint]")
{
    port++;
    faabric::endpoint::FaabricEndpoint endpoint(port, 2);

    std::thread serverThread([&endpoint]() { endpoint.start(false); });

    // Wait for the server to start
    SLEEP_MS(2000);

    // Make the initial invocation
    faabric::Message msg = faabric::util::messageFactory("foo", "blah");
    msg.set_isasync(true);
    std::string body = faabric::util::messageToJson(msg);

    std::pair<int, std::string> result =
      submitGetRequestToUrl(LOCALHOST, port, body);

    REQUIRE(result.first == 200);
    REQUIRE(result.second == std::to_string(msg.id()));

    // Make a status request, should still be running
    faabric::Message statusMsg;
    statusMsg.set_user("foo");
    statusMsg.set_function("blah");
    statusMsg.set_id(msg.id());
    statusMsg.set_isstatusrequest(true);

    std::string statusBody = faabric::util::messageToJson(statusMsg);

    std::pair<int, std::string> statusResult =
      submitGetRequestToUrl(LOCALHOST, port, statusBody);

    REQUIRE(statusResult.first == 200);
    REQUIRE(statusResult.second == "RUNNING");

    // Wait for the function to finish
    SLEEP_MS(4000);

    std::pair<int, std::string> statusResultAfter =
      submitGetRequestToUrl(LOCALHOST, port, statusBody);

    REQUIRE(statusResultAfter.first == 200);
    faabric::Message resultMsg =
      faabric::util::jsonToMessage(statusResultAfter.second);
    REQUIRE(resultMsg.returnvalue() == 0);
    REQUIRE(resultMsg.outputdata() ==
            fmt::format("Finished async message {}", msg.id()));

    endpoint.stop();

    if (serverThread.joinable()) {
        serverThread.join();
    }
}
}
