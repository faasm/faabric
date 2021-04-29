#include <catch.hpp>

#include "faabric_utils.h"

#include <faabric/endpoint/FaabricEndpointHandler.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/json.h>

using namespace Pistache;

namespace tests {
TEST_CASE("Test valid calls to endpoint", "[endpoint]")
{
    cleanFaabric();

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
    const std::string responseStr = handler.handleFunction(requestStr);

    // Check function count has increased
    scheduler::Scheduler& sch = scheduler::getScheduler();
    REQUIRE(sch.getFunctionInFlightCount(call) == 1);

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
    std::string actual = handler.handleFunction("");

    REQUIRE(actual == "Empty request");
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
    std::string actual = handler.handleFunction(requestStr);

    REQUIRE(actual == expected);
}

TEST_CASE("Check getting function status from endpoint", "[endpoint]")
{
    cleanFaabric();

    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();

    // Create a message
    faabric::Message msg = faabric::util::messageFactory("demo", "echo");

    std::string expectedOutput;
    SECTION("Running") { expectedOutput = "RUNNING"; }

    SECTION("Failure")
    {
        std::string errorMsg = "I have failed";
        msg.set_outputdata(errorMsg);
        msg.set_returnvalue(1);
        sch.setFunctionResult(msg);

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
    std::string actual = handler.handleFunction(requestStr);

    REQUIRE(actual == expectedOutput);
}
}
