#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <DummyExecutor.h>
#include <DummyExecutorFactory.h>

#include <faabric/endpoint/FaabricEndpointHandler.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/json.h>

using namespace Pistache;

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

TEST_CASE_METHOD(EndpointHandlerTestFixture,
                 "Test valid calls to endpoint",
                 "[endpoint]")
{
    // Note - must be async to avoid needing a result
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
    endpoint::FaabricEndpointHandler handler;
    std::pair<int, std::string> response = handler.handleFunction(requestStr);

    REQUIRE(response.first == 0);
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
}

TEST_CASE("Test empty invocation", "[endpoint]")
{
    endpoint::FaabricEndpointHandler handler;
    std::pair<int, std::string> actual = handler.handleFunction("");

    REQUIRE(actual.first == 1);
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

    endpoint::FaabricEndpointHandler handler;
    const std::string& requestStr = faabric::util::messageToJson(call);
    std::pair<int, std::string> actual = handler.handleFunction(requestStr);

    REQUIRE(actual.first == 1);
    REQUIRE(actual.second == expected);
}

TEST_CASE_METHOD(EndpointHandlerTestFixture,
                 "Check getting function status from endpoint",
                 "[endpoint]")
{
    // Create a message
    faabric::Message msg = faabric::util::messageFactory("demo", "echo");

    int expectedReturnCode = 0;
    std::string expectedOutput;

    SECTION("Running") { expectedOutput = "RUNNING"; }

    SECTION("Failure")
    {
        std::string errorMsg = "I have failed";
        msg.set_outputdata(errorMsg);
        msg.set_returnvalue(1);
        sch.setFunctionResult(msg);

        expectedReturnCode = 1;

        expectedOutput = "FAILED: " + errorMsg;
    }

    SECTION("Success")
    {
        std::string errorMsg = "I have succeeded";
        msg.set_outputdata(errorMsg);
        msg.set_returnvalue(0);
        sch.setFunctionResult(msg);

        expectedOutput = "SUCCESS: " + errorMsg;
    }

    msg.set_isstatusrequest(true);

    endpoint::FaabricEndpointHandler handler;
    const std::string& requestStr = faabric::util::messageToJson(msg);
    std::pair<int, std::string> actual = handler.handleFunction(requestStr);

    REQUIRE(actual.first == expectedReturnCode);
    REQUIRE(actual.second == expectedOutput);
}
}
