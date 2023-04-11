#include <catch2/catch.hpp>

#include "DummyExecutor.h"
#include "DummyExecutorFactory.h"
#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/endpoint/FaabricEndpointHandler.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/json.h>

namespace tests {

class EndpointHandlerTestFixture : public SchedulerTestFixture
{
  public:
    EndpointHandlerTestFixture()
    {
        executorFactory =
          std::make_shared<faabric::scheduler::DummyExecutorFactory>();
        setExecutorFactory(executorFactory);
    }

    ~EndpointHandlerTestFixture() { executorFactory->reset(); }

  protected:
    std::shared_ptr<faabric::scheduler::DummyExecutorFactory> executorFactory;
};

// Taking in a shared_ptr by reference to ensure the handler was constructed
// with std::make_shared
std::pair<int, std::string> synchronouslyHandleFunction(
  std::shared_ptr<endpoint::FaabricEndpointHandler>& handler,
  std::string requestStr)
{
    asio::io_context ioc(1);
    asio::strand strand = asio::make_strand(ioc);
    faabric::util::BeastHttpResponse response;
    faabric::util::BeastHttpRequest req(beast::http::verb::get, "/", 10);
    req.body() = requestStr;
    faabric::endpoint::HttpRequestContext ctx{
        ioc,
        strand,
        [&](faabric::util::BeastHttpResponse&& resp) {
            response = std::move(resp);
        }
    };
    handler->onRequest(std::move(ctx), std::move(req));
    ioc.run();
    return std::make_pair(response.result_int(), response.body());
}

TEST_CASE_METHOD(EndpointHandlerTestFixture,
                 "Test valid calls to endpoint",
                 "[endpoint]")
{
    // Must be async to avoid needing a result
    faabric::Message call = faabric::util::messageFactory("foo", "bar");
    call.set_isasync(true);
    std::string user = "foo";
    std::string function = "bar";
    std::string actualInput;

    SECTION("With input")
    {
        actualInput = "foobar";
        call.set_inputdata(actualInput);
    }
    SECTION("No input") {}

    call.set_user(user);
    call.set_function(function);

    const std::string& requestStr = faabric::util::messageToJson(call);

    // Handle the function
    std::shared_ptr handler =
      std::make_shared<endpoint::FaabricEndpointHandler>();
    std::pair<int, std::string> response =
      synchronouslyHandleFunction(handler, requestStr);

    REQUIRE(response.first == 200);
    std::string responseStr = response.second;

    // Check actual call has right details including the ID returned to the
    // caller
    std::vector<faabric::Message> msgs = sch.getRecordedMessagesAll();
    REQUIRE(msgs.size() == 1);
    faabric::Message actualCall = msgs.at(0);
    REQUIRE(actualCall.user() == call.user());
    REQUIRE(actualCall.function() == call.function());
    REQUIRE(actualCall.id() == std::stoi(responseStr));
    REQUIRE(actualCall.inputdata() == actualInput);

    // Wait for the result
    sch.getFunctionResult(actualCall.id(), 2000);
}

TEST_CASE("Test empty invocation", "[endpoint]")
{
    std::shared_ptr handler =
      std::make_shared<endpoint::FaabricEndpointHandler>();
    std::pair<int, std::string> actual =
      synchronouslyHandleFunction(handler, "");

    REQUIRE(actual.first == 400);
    REQUIRE(actual.second == "Empty request");
}

TEST_CASE("Test empty JSON invocation", "[endpoint]")
{
    faabric::Message call;
    call.set_isasync(true);

    std::string expected;

    SECTION("Empty user")
    {
        expected = "Empty user";
        call.set_function("echo");
    }

    SECTION("Empty function")
    {
        expected = "Empty function";
        call.set_user("demo");
    }

    std::shared_ptr handler =
      std::make_shared<endpoint::FaabricEndpointHandler>();
    const std::string& requestStr = faabric::util::messageToJson(call);
    std::pair<int, std::string> actual =
      synchronouslyHandleFunction(handler, requestStr);

    REQUIRE(actual.first == 400);
    REQUIRE(actual.second == expected);
}

TEST_CASE_METHOD(EndpointHandlerTestFixture,
                 "Check getting function status from endpoint",
                 "[endpoint]")
{
    // Create a message
    faabric::Message msg = faabric::util::messageFactory("demo", "echo");

    int expectedReturnCode = 200;
    std::string expectedOutput;

    SECTION("Running") { expectedOutput = "RUNNING"; }

    SECTION("Failure")
    {
        std::string errorMsg = "I have failed";
        msg.set_outputdata(errorMsg);
        msg.set_returnvalue(1);
        sch.setFunctionResult(msg);

        expectedReturnCode = 500;

        expectedOutput = "FAILED: " + errorMsg;
    }

    SECTION("Success")
    {
        std::string errorMsg = "I have succeeded";
        msg.set_outputdata(errorMsg);
        msg.set_returnvalue(0);
        sch.setFunctionResult(msg);

        expectedOutput = faabric::util::messageToJson(msg);
    }

    msg.set_isstatusrequest(true);

    std::shared_ptr handler =
      std::make_shared<endpoint::FaabricEndpointHandler>();
    const std::string& requestStr = faabric::util::messageToJson(msg);
    std::pair<int, std::string> actual =
      synchronouslyHandleFunction(handler, requestStr);

    REQUIRE(actual.first == expectedReturnCode);
    REQUIRE(actual.second == expectedOutput);
}
}
