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
    auto& cli = getPlannerClient();
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
    auto& cli = getPlannerClient();
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

    void waitForBerToFinish(std::shared_ptr<BatchExecuteRequest> ber)
    {
        HttpMessage statusMsg;
        statusMsg.set_type(HttpMessage_Type_EXECUTE_BATCH_STATUS);
        auto berStatus = faabric::util::batchExecStatusFactory(ber->appid());
        berStatus->set_expectednummessages(ber->messages_size());
        statusMsg.set_payloadjson(faabric::util::messageToJson(*berStatus));
        std::string statusMsgStr = faabric::util::messageToJson(statusMsg);

        // Poll the planner a number of times (it may be that the app is not
        // registered in the results yet, so we consider that case too)
        bool hasFinished = false;
        std::pair<int, std::string> result;
        for (int i = 0; i < 5; i++) {
            int timeoutMs = 200;
            result = doPost(statusMsgStr);

            if (boost::beast::http::int_to_status(result.first) ==
                beast::http::status::internal_server_error) {
                // Such an error could be because the app is not registered in
                // the results yet, we are fine with that
                SLEEP_MS(timeoutMs);
                continue;
            }

            REQUIRE(boost::beast::http::int_to_status(result.first) ==
                    beast::http::status::ok);
            BatchExecuteRequestStatus actualBerStatus;
            faabric::util::jsonToMessage(result.second, &actualBerStatus);

            if (actualBerStatus.finished()) {
                hasFinished = true;
            }

            SLEEP_MS(timeoutMs);
        }

        if (!hasFinished) {
            SPDLOG_ERROR("Timed-out waiting for app {} to finish!",
                         ber->appid());
            throw std::runtime_error("Timed-out waiting for app to finish");
        }
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
    plannerCli.callFunctions(ber);
    auto resultMsg = getPlannerClient().getMessageResult(appId, msgId, 1000);

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

    auto msgResult = getPlannerClient().getMessageResult(appId, msgId, 1000);
    REQUIRE(msgResult.returnvalue() == 0);

    // If the request is succesful, check that the response has the fields
    // we expect
    if (expectedReturnCode == beast::http::status::ok) {
        REQUIRE(msgResult.timestamp() > 0);
        REQUIRE(msgResult.finishtimestamp() > 0);
        REQUIRE(!msgResult.executedhost().empty());
        REQUIRE(!msgResult.mainhost().empty());
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
    auto msgResult = getPlannerClient().getMessageResult(appId, msgId, 1000);
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

    // Post the EXECUTE_BATCH_STATUS request:
    msgJsonStr = faabric::util::messageToJson(msg);
    result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);
}

TEST_CASE_METHOD(PlannerEndpointExecTestFixture,
                 "Test flushing the scheduling state",
                 "[planner]")
{
    // Here we use mock mode to make sure no actual execution calls are
    // dispatched
    faabric::util::setMockMode(true);

    int numMessages = 5;
    HttpMessage msg;
    msg.set_type(HttpMessage_Type_EXECUTE_BATCH);
    auto ber = faabric::util::batchExecFactory("foo", "bar", numMessages);
    msg.set_payloadjson(faabric::util::messageToJson(*ber));
    msgJsonStr = faabric::util::messageToJson(msg);
    std::pair<int, std::string> result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            beast::http::status::ok);

    SECTION("Flush via reset") { resetPlanner(); }

    SECTION("Flush via specific flush message")
    {
        HttpMessage flushMsg;
        msg.set_type(HttpMessage_Type_FLUSH_SCHEDULING_STATE);
        msgJsonStr = faabric::util::messageToJson(msg);
        std::pair<int, std::string> result = doPost(msgJsonStr);
        std::string expectedResponse = "Flushed scheduling state!";
        REQUIRE(boost::beast::http::int_to_status(result.first) ==
                beast::http::status::ok);
        REQUIRE(result.second == expectedResponse);
    }

    // After flushing the scheduling state, there should be no apps in flight
    HttpMessage inFlightMsg;
    inFlightMsg.set_type(HttpMessage_Type_GET_IN_FLIGHT_APPS);
    GetInFlightAppsResponse expectedResponse;
    msgJsonStr = faabric::util::messageToJson(inFlightMsg);
    expectedResponseBody = faabric::util::messageToJson(expectedResponse);
    expectedReturnCode = beast::http::status::ok;
    result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);

    faabric::util::setMockMode(false);
}

TEST_CASE_METHOD(PlannerEndpointExecTestFixture,
                 "Check getting all in-flight apps through endpoint",
                 "[planner]")
{
    // First, prepare an HTTP request to execute a batch
    int numMessages = 5;
    HttpMessage msg;
    msg.set_type(HttpMessage_Type_EXECUTE_BATCH);
    auto ber = faabric::util::batchExecFactory("foo", "bar", numMessages);
    int appId = ber->appid();
    msg.set_payloadjson(faabric::util::messageToJson(*ber));

    // At the begining, there should be no in-flight apps
    HttpMessage inFlightMsg;
    inFlightMsg.set_type(HttpMessage_Type_GET_IN_FLIGHT_APPS);
    GetInFlightAppsResponse expectedResponse;
    expectedResponse.set_nummigrations(0);
    msgJsonStr = faabric::util::messageToJson(inFlightMsg);
    expectedResponseBody = faabric::util::messageToJson(expectedResponse);
    expectedReturnCode = beast::http::status::ok;

    // Post the first in-flight request
    std::pair<int, std::string> result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);

    // Now prepare the expected in-flight response once we schedule one batch
    auto* inFlightApp = expectedResponse.add_apps();
    inFlightApp->set_appid(appId);
    for (int i = 0; i < ber->messages_size(); i++) {
        inFlightApp->add_hostips(conf.endpointHost);
    }
    expectedResponseBody = faabric::util::messageToJson(expectedResponse);
    expectedReturnCode = beast::http::status::ok;

    // Execute the batch
    msgJsonStr = faabric::util::messageToJson(msg);
    result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            beast::http::status::ok);

    // Get the in-flight app another time, now there should be one app
    msg.set_type(HttpMessage_Type_GET_IN_FLIGHT_APPS);
    msgJsonStr = faabric::util::messageToJson(msg);
    result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);

    // After the batch has finished executing, there should be no more
    // in-flight apps (we poll the planner until all messages have finished)
    waitForBerToFinish(ber);

    // Once we are sure the batch has finished, check again that there are
    // zero apps in-flight
    GetInFlightAppsResponse emptyExpectedResponse;
    msgJsonStr = faabric::util::messageToJson(inFlightMsg);
    expectedResponseBody = faabric::util::messageToJson(emptyExpectedResponse);
    result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            beast::http::status::ok);
    REQUIRE(result.second == expectedResponseBody);
}

TEST_CASE_METHOD(PlannerEndpointExecTestFixture,
                 "Check pre-loading a scheduling decision through endpoint",
                 "[planner]")
{
    // First, prepare an HTTP request to execute a batch
    int numMessages = 5;
    HttpMessage msg;
    msg.set_type(HttpMessage_Type_EXECUTE_BATCH);
    auto ber = faabric::util::batchExecFactory("foo", "bar", numMessages);
    msg.set_payloadjson(faabric::util::messageToJson(*ber));
    msgJsonStr = faabric::util::messageToJson(msg);

    // Prepare the preload message
    HttpMessage preloadMsg;
    preloadMsg.set_type(HttpMessage_Type_PRELOAD_SCHEDULING_DECISION);
    for (int i = 0; i < ber->messages_size(); i++) {
        auto& msg = ber->mutable_messages()->at(i);
        msg.set_executedhost(faabric::util::getSystemConfig().endpointHost);
        msg.set_groupidx(i);
    }

    SECTION("Valid request")
    {
        preloadMsg.set_payloadjson(faabric::util::messageToJson(*ber));
        expectedResponseBody = "Decision pre-loaded to planner";
        expectedReturnCode = beast::http::status::ok;
    }

    SECTION("Bad request payload")
    {
        preloadMsg.set_payloadjson("foo bar");
        expectedReturnCode = beast::http::status::bad_request;
        expectedResponseBody = "Bad JSON in request body";
    }

    // Post the preload HTTP request
    std::string preloadMsgJsonStr = faabric::util::messageToJson(preloadMsg);
    std::pair<int, std::string> result = doPost(preloadMsgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);

    // Now execute the batch
    result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            beast::http::status::ok);

    // Wait for BER to finish
    waitForBerToFinish(ber);
}
}
