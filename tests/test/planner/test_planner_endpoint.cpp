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
        resetPlanner();
    }

    ~PlannerEndpointTestFixture()
    {
        resetPlanner();
        endpoint.stop();
    }

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
    msg.set_type(HttpMessage_Type_FLUSH_AVAILABLE_HOSTS);
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
                 "Test getting the available hosts via endpoint",
                 "[planner]")
{
    // First flush the available hosts
    HttpMessage flushMsg;
    flushMsg.set_type(HttpMessage_Type_FLUSH_AVAILABLE_HOSTS);
    std::string flushMsgStr = faabric::util::messageToJson(flushMsg);
    std::pair<int, std::string> result = doPost(flushMsgStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            boost::beast::http::status::ok);

    AvailableHostsResponse hostsResponse;

    // Now, getting the available hosts should return zero hosts
    HttpMessage getMsg;
    getMsg.set_type(HttpMessage_Type_GET_AVAILABLE_HOSTS);
    std::string getMsgStr = faabric::util::messageToJson(getMsg);
    result = doPost(getMsgStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            boost::beast::http::status::ok);
    REQUIRE_NOTHROW(faabric::util::jsonToMessage(result.second, &getMsg));
    REQUIRE(hostsResponse.hosts().empty());

    // If we register a number of hosts, we are able to retrieve them through
    // the API
    AvailableHostsResponse expectedHostsResponse;
    SECTION("One host")
    {
        auto* host = expectedHostsResponse.add_hosts();
        host->set_ip("foo");
        host->set_slots(12);
    }

    SECTION("Multiple hosts")
    {
        auto* host = expectedHostsResponse.add_hosts();
        // The planner will return the hosts in alphabetical order by IP, so
        // we sort them here too
        host = expectedHostsResponse.add_hosts();
        host->set_ip("bar");
        host->set_slots(4);
        host = expectedHostsResponse.add_hosts();
        host->set_ip("baz");
        host->set_slots(7);
        host = expectedHostsResponse.add_hosts();
        host->set_ip("foo");
        host->set_slots(12);
    }

    // Register the hosts
    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    PlannerClient cli;
    for (auto host : expectedHostsResponse.hosts()) {
        *regReq->mutable_host() = host;
        cli.registerHost(regReq);
    }

    // Post HTTP request and check it matches the expectation
    result = doPost(getMsgStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            boost::beast::http::status::ok);
    REQUIRE_NOTHROW(
      faabric::util::jsonToMessage(result.second, &hostsResponse));
    REQUIRE(hostsResponse.hosts().size() ==
            expectedHostsResponse.hosts().size());
    for (int i = 0; i < hostsResponse.hosts().size(); i++) {
        REQUIRE(hostsResponse.hosts(i).ip() ==
                expectedHostsResponse.hosts(i).ip());
        REQUIRE(hostsResponse.hosts(i).slots() ==
                expectedHostsResponse.hosts(i).slots());
    }
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
        sch.shutdown();
        sch.addHostToGlobalSet();

        std::shared_ptr<faabric::scheduler::ExecutorFactory> fac =
          std::make_shared<faabric::scheduler::DummyExecutorFactory>();
        faabric::scheduler::setExecutorFactory(fac);
    }

    ~PlannerEndpointExecTestFixture()
    {
        sch.shutdown();
        sch.addHostToGlobalSet();
    }

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
        auto expectedBerStatus = faabric::util::batchExecStatusFactory(appId);
        expectedResponseBody = faabric::util::messageToJson(*expectedBerStatus);
    }

    // The EXECUTE_BATCH request requires a serialised BatchExecuteRequest as
    // payload, otherwise it will return an error
    SECTION("Bad request payload")
    {
        msg.set_payloadjson("foo bar");
        expectedReturnCode = beast::http::status::bad_request;
        expectedResponseBody = "Bad JSON in body's payload";
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

    // IF the BER does not pass the sanity checks, the endpoint will error
    SECTION("Bad BER body")
    {
        expectedReturnCode = beast::http::status::bad_request;
        expectedResponseBody = "Bad BatchExecRequest";

        SECTION("Bad ber id") { ber->set_appid(1337); }

        SECTION("App ID mismatch")
        {
            ber->mutable_messages(0)->set_appid(1337);
        }

        SECTION("Empty user") { ber->mutable_messages(0)->set_user(""); }

        SECTION("Empty function")
        {
            ber->mutable_messages(0)->set_function("");
        }

        msg.set_payloadjson(faabric::util::messageToJson(*ber));
    }

    // Post the message that will trigger a function execution
    msgJsonStr = faabric::util::messageToJson(msg);
    std::pair<int, std::string> result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);

    auto msgResult = sch.getFunctionResult(appId, msgId, 1000);
    REQUIRE(msgResult.returnvalue() == 0);

    // If the request is succesful, check that the response has the fields
    // we expect
    if (expectedReturnCode == beast::http::status::ok) {
        REQUIRE(msgResult.timestamp() > 0);
        REQUIRE(msgResult.finishtimestamp() > 0);
        REQUIRE(!msgResult.executedhost().empty());
        REQUIRE(!msgResult.masterhost().empty());
    }
}

TEST_CASE_METHOD(PlannerEndpointExecTestFixture,
                 "Check getting the execution status through the endpoint",
                 "[planner]")
{
    // First, prepare an HTTP request to execute a batch
    int numMessages = 1;
    HttpMessage msg;
    msg.set_type(HttpMessage_Type_EXECUTE_BATCH);
    auto ber = faabric::util::batchExecFactory("foo", "bar", numMessages);
    int appId = ber->appid();
    int msgId = ber->messages(0).id();
    msg.set_payloadjson(faabric::util::messageToJson(*ber));

    // Execute the batch
    msgJsonStr = faabric::util::messageToJson(msg);
    std::pair<int, std::string> result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            beast::http::status::ok);

    // Make sure execution has finished and the result is available
    auto msgResult = sch.getFunctionResult(appId, msgId, 1000);
    REQUIRE(msgResult.returnvalue() == 0);

    // Second, prepare an HTTP request to get the batch's execution status
    msg.set_type(HttpMessage_Type_EXECUTE_BATCH_STATUS);
    // An EXECUTE_BATCH_STATUS request needs to provide a serialised
    // BatchExecuteRequestStatus in the request's JSON payload
    auto berStatus = faabric::util::batchExecStatusFactory(appId);
    berStatus->set_expectednummessages(numMessages);
    msg.set_payloadjson(faabric::util::messageToJson(*berStatus));

    // An EXECUTE_BATCH_STATUS request expects a BatchExecuteRequestStatus
    // in the response body
    SECTION("Success")
    {
        expectedReturnCode = beast::http::status::ok;
        auto expectedBerStatus = faabric::util::batchExecStatusFactory(appId);
        expectedBerStatus->set_finished(true);
        *expectedBerStatus->add_messageresults() = msgResult;
        expectedResponseBody = faabric::util::messageToJson(*expectedBerStatus);
    }

    // If the request JSON payload is not a BER, the endpoint will error
    SECTION("Malformed request body")
    {
        expectedReturnCode = beast::http::status::bad_request;
        expectedResponseBody = "Bad JSON in request body";
        msg.set_payloadjson("foo");
    }

    // If the request JSON payload contains a BER status for a non-existant
    // BER (i.e. appid not registered), the endpoint will also error out
    SECTION("Unregistered app id")
    {
        expectedReturnCode = beast::http::status::internal_server_error;
        expectedResponseBody = "App not registered in results";
        auto otherBerStatus = faabric::util::batchExecStatusFactory(1337);
        msg.set_payloadjson(faabric::util::messageToJson(*otherBerStatus));
    }

    // If the request JSON payload contains a BER status for an in-flight BER,
    // the request will succeed. Depending on the messages we tell the planner
    // we are expecting, it will either succeed or not
    SECTION("Success, but not finished")
    {
        expectedReturnCode = beast::http::status::ok;
        auto expectedBerStatus = faabric::util::batchExecStatusFactory(appId);
        expectedBerStatus->set_finished(false);
        *expectedBerStatus->add_messageresults() = msgResult;
        expectedResponseBody = faabric::util::messageToJson(*expectedBerStatus);
        // Change the expected number of messages
        berStatus->set_expectednummessages(2);
        msg.set_payloadjson(faabric::util::messageToJson(*berStatus));
    }

    // Post the EXECUTE_BATCH_STATUS request:
    msgJsonStr = faabric::util::messageToJson(msg);
    result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);
}
}
