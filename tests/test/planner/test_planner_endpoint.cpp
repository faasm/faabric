#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/PlannerEndpointHandler.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/util/json.h>

#include <boost/beast/http/status.hpp>

using namespace faabric::planner;

namespace tests {
class PlannerEndpointTestFixture
  : public ConfFixture
  , public PlannerClientServerFixture
{
  public:
    PlannerEndpointTestFixture()
      : host(LOCALHOST)
      , port(conf.plannerPort)
      , endpoint(
          port,
          faabric::planner::getPlanner().getConfig().numthreadshttpserver(),
          std::make_shared<faabric::planner::PlannerEndpointHandler>())
    {
        conf.plannerHost = LOCALHOST;
        endpoint.start(faabric::endpoint::EndpointMode::BG_THREAD);
    }

    ~PlannerEndpointTestFixture() { endpoint.stop(); }

  protected:
    std::string host;
    int port;
    faabric::endpoint::FaabricEndpoint endpoint;

    // Test case state
    boost::beast::http::status expectedReturnCode;
    std::string expectedResponseBody;
    std::string msgJsonStr;

    std::pair<int, std::string> doPost(const std::string& body)
    {
        return postToUrl(host, port, body);
    }
};

TEST_CASE_METHOD(PlannerEndpointTestFixture, "Test planner reset", "[planner]")
{
    expectedReturnCode = boost::beast::http::status::ok;
    expectedResponseBody = "Planner fully reset!";

    HttpMessage msg;
    msg.set_type(HttpMessage_Type_RESET);

    msgJsonStr = faabric::util::messageToJson(msg);

    std::pair<int, std::string> result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);

    // Check that the set of available hosts is empty after reset
    std::vector<faabric::planner::Host> availableHosts =
      plannerCli.getAvailableHosts();
    REQUIRE(availableHosts.empty());

    // Add a host, reset, and check again
    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    regReq->mutable_host()->set_ip("foo");
    regReq->mutable_host()->set_slots(12);
    plannerCli.registerHost(regReq);
    availableHosts = plannerCli.getAvailableHosts();
    REQUIRE(availableHosts.size() == 1);

    // Reset again
    result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);

    // Check count is now zero again
    availableHosts = plannerCli.getAvailableHosts();
    REQUIRE(availableHosts.empty());
}

TEST_CASE_METHOD(PlannerEndpointTestFixture,
                 "Test flushing available hosts",
                 "[planner]")
{
    expectedReturnCode = boost::beast::http::status::ok;
    expectedResponseBody = "Flushed available hosts!";

    // Prepare the message
    HttpMessage msg;
    msg.set_type(HttpMessage_Type_FLUSH_HOSTS);
    msgJsonStr = faabric::util::messageToJson(msg);

    // Send it
    std::pair<int, std::string> result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);

    // Check that, initially, there are no available hosts
    PlannerClient cli;
    std::vector<faabric::planner::Host> availableHosts =
      cli.getAvailableHosts();
    REQUIRE(availableHosts.empty());

    // Add a host, reset, and check again
    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    regReq->mutable_host()->set_ip("foo");
    regReq->mutable_host()->set_slots(12);
    cli.registerHost(regReq);
    availableHosts = cli.getAvailableHosts();
    REQUIRE(availableHosts.size() == 1);

    // Reset again
    result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);

    // Check count is now zero again
    availableHosts = cli.getAvailableHosts();
    REQUIRE(availableHosts.empty());
}

TEST_CASE_METHOD(PlannerEndpointTestFixture,
                 "Test flushing executors",
                 "[planner]")
{
    faabric::util::setMockMode(true);

    std::string hostA = "alpha";
    std::string hostB = "beta";
    std::string hostC = "gamma";
    auto hostReqA = std::make_shared<faabric::planner::RegisterHostRequest>();
    hostReqA->mutable_host()->set_ip(hostA);
    auto hostReqB = std::make_shared<faabric::planner::RegisterHostRequest>();
    hostReqB->mutable_host()->set_ip(hostB);
    auto hostReqC = std::make_shared<faabric::planner::RegisterHostRequest>();
    hostReqC->mutable_host()->set_ip(hostC);

    std::vector<std::string> expectedHosts = { hostA, hostB, hostC };

    plannerCli.registerHost(hostReqA);
    plannerCli.registerHost(hostReqB);
    plannerCli.registerHost(hostReqC);

    // Send flush request
    HttpMessage msg;
    expectedReturnCode = boost::beast::http::status::ok;
    expectedResponseBody = "Flushed executors!";
    msg.set_type(HttpMessage_Type_FLUSH_EXECUTORS);
    msgJsonStr = faabric::util::messageToJson(msg);
    std::pair<int, std::string> result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);

    // Make sure messages have been sent
    auto calls = faabric::scheduler::getFlushCalls();
    REQUIRE(calls.size() == 3);

    std::vector<std::string> actualHosts;
    for (auto c : calls) {
        actualHosts.emplace_back(c.first);
    }

    REQUIRE(expectedHosts == actualHosts);

    faabric::util::setMockMode(false);
    faabric::scheduler::clearMockRequests();
    faabric::scheduler::getScheduler().shutdown();
}

TEST_CASE_METHOD(PlannerEndpointTestFixture,
                 "Test getting the planner config",
                 "[planner]")
{
    expectedReturnCode = boost::beast::http::status::ok;

    HttpMessage msg;
    msg.set_type(HttpMessage_Type_GET_CONFIG);
    msgJsonStr = faabric::util::messageToJson(msg);

    // Send it
    std::pair<int, std::string> result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);

    // Check that we can de-serialise the config. Note that if there's a
    // de-serialisation the method will throw an exception
    PlannerConfig config;
    REQUIRE_NOTHROW(faabric::util::jsonToMessage(result.second, &config));
    REQUIRE(!config.ip().empty());
    REQUIRE(config.hosttimeout() > 0);
    REQUIRE(config.numthreadshttpserver() > 0);
}

class PlannerEndpointExecTestFixture
  : public PlannerEndpointTestFixture
  , public FunctionCallClientServerFixture
{
  public:
    PlannerEndpointExecTestFixture()
      : sch(faabric::scheduler::getScheduler())
    {
        sch.reset();
        sch.addHostToGlobalSet();

        std::shared_ptr<faabric::scheduler::ExecutorFactory> fac =
          std::make_shared<faabric::scheduler::DummyExecutorFactory>();
        faabric::scheduler::setExecutorFactory(fac);
    }

    ~PlannerEndpointExecTestFixture() { sch.shutdown(); }

  protected:
    faabric::scheduler::Scheduler& sch;
};

TEST_CASE_METHOD(PlannerEndpointExecTestFixture,
                 "Check getting execution graph from endpoint",
                 "[planner]")
{
    // Prepare HTTP Message
    HttpMessage msg;
    msg.set_type(HttpMessage_Type_GET_EXEC_GRAPH);
    auto ber = faabric::util::batchExecFactory("foo", "bar", 1);
    int appId = ber->messages(0).appid();
    int msgId = ber->messages(0).id();
    msg.set_payloadjson(faabric::util::messageToJson(ber->messages(0)));

    // Call a function first, and wait for the result
    sch.callFunctions(ber);
    auto resultMsg = sch.getFunctionResult(appId, msgId, 1000);

    SECTION("Success")
    {
        expectedReturnCode = boost::beast::http::status::ok;
        faabric::util::ExecGraphNode rootNode = { .msg = resultMsg };
        faabric::util::ExecGraph expectedGraph{ .rootNode = rootNode };
        expectedResponseBody = faabric::util::execGraphToJson(expectedGraph);
    }

    // If we can't find the exec. graph, the endpoint will return an error
    SECTION("Failure")
    {
        ber->mutable_messages(0)->set_appid(1337);
        msg.set_payloadjson(faabric::util::messageToJson(ber->messages(0)));
        expectedReturnCode = beast::http::status::internal_server_error;
        expectedResponseBody = "Failed getting exec. graph!";
    }

    // The GET_EXEC_GRAPH request requires a serialised Message as
    // payload, otherwise it will return an error
    SECTION("Bad request payload")
    {
        msg.set_payloadjson("foo bar");
        expectedReturnCode = beast::http::status::bad_request;
        expectedResponseBody = "Bad JSON in request body";
    }

    // Send an HTTP request to get the execution graph
    msgJsonStr = faabric::util::messageToJson(msg);
    std::pair<int, std::string> result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);
}

TEST_CASE_METHOD(PlannerEndpointExecTestFixture,
                 "Check executing a function through the endpoint",
                 "[planner]")
{
    // Prepare HTTP request
    HttpMessage msg;
    msg.set_type(HttpMessage_Type_EXECUTE_BATCH);
    auto ber = faabric::util::batchExecFactory("foo", "bar", 1);
    int appId = ber->appid();
    int msgId = ber->messages(0).id();
    msg.set_payloadjson(faabric::util::messageToJson(*ber));

    SECTION("Success")
    {
        expectedReturnCode = beast::http::status::ok;
        faabric::BatchExecuteRequestStatus expectedBerStatus;
        expectedBerStatus.set_appid(appId);
        expectedBerStatus.set_finished(false);
        expectedResponseBody = faabric::util::messageToJson(expectedBerStatus);
    }

    // The EXECUTE_BATCH request requires a serialised BatchExecuteRequest as
    // payload, otherwise it will return an error
    SECTION("Bad request payload")
    {
        msg.set_payloadjson("foo bar");
        expectedReturnCode = beast::http::status::bad_request;
        expectedResponseBody = "Bad JSON in request body";
    }

    // Trying to execute a function without any registered hosts should yield
    // an error
    SECTION("No registered hosts")
    {
        expectedReturnCode = beast::http::status::internal_server_error;
        expectedResponseBody = "No available hosts";
        // Remove all registered hosts
        resetPlanner();
    }

    /*
    SECTION("Bad request body")
    {
        expectedReturnCode = beast::http::status::internal_server_error;
    }
    */

    // Post the message that will trigger a function execution
    msgJsonStr = faabric::util::messageToJson(msg);
    std::pair<int, std::string> result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);

    auto msgResult = sch.getFunctionResult(appId, msgId, 1000);
    REQUIRE(msgResult.returnvalue() == 0);
}
}
