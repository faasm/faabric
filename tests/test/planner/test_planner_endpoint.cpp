#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/util/json.h>

#include <boost/beast/http/status.hpp>

using namespace faabric::planner;

/* This tests use that the planner is already running as a standalone service
 * and only test the external HTTP API
 */
namespace tests {
class FaabricPlannerEndpointTestFixture : public ConfTestFixture
{
  public:
    FaabricPlannerEndpointTestFixture()
      : host(conf.plannerHost)
      , port(conf.plannerPort)
    {}

  protected:
    // Planner endpoint state
    std::string host;
    int port;

    // Test case state
    boost::beast::http::status expectedReturnCode;
    std::string expectedResponseBody;
    std::string msgJsonStr;

    std::pair<int, std::string> doPost(const std::string& body)
    {
        return postToUrl(host, port, body);
    }
};

TEST_CASE_METHOD(FaabricPlannerEndpointTestFixture,
                 "Test planner reset",
                 "[planner]")
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

TEST_CASE_METHOD(FaabricPlannerEndpointTestFixture,
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

TEST_CASE_METHOD(FaabricPlannerEndpointTestFixture,
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

TEST_CASE_METHOD(FaabricPlannerEndpointTestFixture,
                 "Test flushing executors with HTTP request",
                 "[planner]")
{
    // Reset planner
    HttpMessage msg;
    msg.set_type(HttpMessage_Type_RESET);
    msgJsonStr = faabric::util::messageToJson(msg);
    std::pair<int, std::string> result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) == boost::beast::http::status::ok);

    expectedReturnCode = boost::beast::http::status::ok;
    expectedResponseBody = "Flushed executors!";

    // To test the flushing of executors, we need to set up a functioning
    // executor pool, which requires a PointToPoint and a FunctionCall server,
    // and an executor factory
    faabric::scheduler::FunctionCallServer functionCallServer;
    faabric::transport::PointToPointServer pointToPointServer;
    auto executorFactory = std::make_shared<faabric::scheduler::DummyExecutorFactory>();
    auto& sch = faabric::scheduler::getScheduler();
    sch.reset();
    setExecutorFactory(executorFactory);
    functionCallServer.start();
    pointToPointServer.start();

    // Prepare the flush message
    msg.set_type(HttpMessage_Type_FLUSH_EXECUTORS);
    msgJsonStr = faabric::util::messageToJson(msg);

    // Set up this host
    faabric::scheduler::getScheduler().addHostToGlobalSet();

    // Set up some state
    faabric::state::State& state = faabric::state::getGlobalState();
    state.getKV("demo", "blah", 10);
    state.getKV("other", "foo", 30);

    // Execute a couple of functions
    auto reqA = faabric::util::batchExecFactory("dummy", "foo", 1);
    auto msgA = reqA->messages().at(0);
    auto reqB = faabric::util::batchExecFactory("dummy", "bar", 1);
    auto msgB = reqB->messages().at(0);
    sch.callFunctions(reqA);
    sch.callFunctions(reqB);

    // Check executor state is set
    REQUIRE(state.getKVCount() == 2);

    // Wait for functions to finish
    sch.getFunctionResult(msgA, 2000);
    sch.getFunctionResult(msgB, 2000);

    // Check messages passed
    std::vector<faabric::Message> msgs = sch.getRecordedMessagesAll();
    REQUIRE(msgs.size() == 2);
    REQUIRE(msgs.at(0).function() == "foo");
    REQUIRE(msgs.at(1).function() == "bar");
    sch.clearRecordedMessages();

    // Check executors present
    REQUIRE(sch.getFunctionExecutorCount(msgA) == 1);
    REQUIRE(sch.getFunctionExecutorCount(msgB) == 1);

    // Send it
    result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);

    // Check executor state is cleared
    REQUIRE(state.getKVCount() == 0);

    // Check the scheduler has been flushed
    REQUIRE(sch.getFunctionExecutorCount(msgA) == 0);
    REQUIRE(sch.getFunctionExecutorCount(msgB) == 0);

    // Check the flush hook has been called
    int flushCount = executorFactory->getFlushCount();
    REQUIRE(flushCount == 1);

    // Clean-up all the mocked-up state
    functionCallServer.stop();
    pointToPointServer.stop();
    executorFactory->reset();
    sch.shutdown();

    // Reset again
    msg.set_type(HttpMessage_Type_RESET);
    msgJsonStr = faabric::util::messageToJson(msg);
    result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) == boost::beast::http::status::ok);
}
}
