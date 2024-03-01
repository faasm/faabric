#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/util/json.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

using namespace faabric::planner;

namespace tests {
TEST_CASE_METHOD(PlannerClientServerFixture,
                 "Test sending ping to planner",
                 "[planner]")
{
    auto& cli = getPlannerClient();
    REQUIRE_NOTHROW(cli.ping());
}

TEST_CASE_METHOD(PlannerClientServerFixture,
                 "Test registering host",
                 "[planner]")
{
    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    regReq->mutable_host()->set_ip("foo");
    regReq->mutable_host()->set_slots(12);
    int plannerTimeout;

    REQUIRE_NOTHROW(plannerTimeout = plannerCli.registerHost(regReq));

    // A call to register a host returns the keep-alive timeout
    REQUIRE(plannerTimeout > 0);

    // We can register the host again, and get the same timeout
    int newTimeout;
    REQUIRE_NOTHROW(newTimeout = plannerCli.registerHost(regReq));
    REQUIRE(newTimeout == plannerTimeout);
}

TEST_CASE_METHOD(PlannerClientServerFixture,
                 "Test getting the available hosts",
                 "[planner]")
{
    // We can ask for the number of available hosts even if no host has been
    // registered, initially there's 0 available hosts
    std::vector<faabric::planner::Host> availableHosts;
    REQUIRE_NOTHROW(availableHosts = plannerCli.getAvailableHosts());
    REQUIRE(availableHosts.empty());

    // Registering one host increases the count by one
    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    regReq->mutable_host()->set_ip("foo");
    regReq->mutable_host()->set_slots(12);
    plannerCli.registerHost(regReq);

    availableHosts = plannerCli.getAvailableHosts();
    REQUIRE(availableHosts.size() == 1);

    // If we wait more than the timeout, the host will have expired. We sleep
    // for twice the timeout
    int timeToSleep = getPlanner().getConfig().hosttimeout() * 2;
    SPDLOG_INFO(
      "Sleeping for {} seconds (twice the timeout) to ensure entries expire",
      timeToSleep);
    SLEEP_MS(timeToSleep * 1000);
    availableHosts = plannerCli.getAvailableHosts();
    REQUIRE(availableHosts.empty());
}

TEST_CASE_METHOD(PlannerClientServerFixture,
                 "Test removing a host",
                 "[planner]")
{
    Host thisHost;
    thisHost.set_ip("foo");
    thisHost.set_slots(12);

    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    *regReq->mutable_host() = thisHost;
    plannerCli.registerHost(regReq);

    std::vector<Host> availableHosts = plannerCli.getAvailableHosts();
    REQUIRE(availableHosts.size() == 1);

    auto remReq = std::make_shared<faabric::planner::RemoveHostRequest>();
    *remReq->mutable_host() = thisHost;
    plannerCli.removeHost(remReq);
    availableHosts = plannerCli.getAvailableHosts();
    REQUIRE(availableHosts.empty());
}

TEST_CASE_METHOD(PlannerClientServerFixture,
                 "Test setting/getting message results",
                 "[planner]")
{
    faabric::util::setMockMode(true);

    auto msgPtr = std::make_shared<faabric::Message>(
      faabric::util::messageFactory("foo", "bar"));

    // Register a host with the planner so that we can set/get results from it
    std::string hostIp = "foo";
    Host thisHost;
    thisHost.set_ip(hostIp);
    thisHost.set_usedslots(2);
    thisHost.set_slots(12);
    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    *regReq->mutable_host() = thisHost;
    plannerCli.registerHost(regReq);

    // If we try to get the message result before setting it first, nothing
    // happens (note that we need to pass a 0 timeout to not block)
    auto resultMsg =
      plannerCli.getMessageResult(msgPtr->appid(), msgPtr->id(), 0);
    REQUIRE(resultMsg.type() == faabric::Message_MessageType_EMPTY);

    // If we set the message result, then we can get it (note that we read it
    // from the mocked requests)
    int expectedReturnValue = 1337;
    msgPtr->set_returnvalue(expectedReturnValue);
    msgPtr->set_executedhost(hostIp);
    plannerCli.setMessageResult(msgPtr);
    SLEEP_MS(500);

    // Read from mocked requests
    auto msgResults = faabric::scheduler::getMessageResults();
    REQUIRE(msgResults.size() == 1);
    REQUIRE(msgResults.at(0).first ==
            faabric::util::getSystemConfig().endpointHost);
    REQUIRE(msgResults.at(0).second->type() !=
            faabric::Message_MessageType_EMPTY);
    REQUIRE(msgResults.at(0).second->id() == msgPtr->id());
    REQUIRE(msgResults.at(0).second->appid() == msgPtr->appid());
    REQUIRE(msgResults.at(0).second->returnvalue() == expectedReturnValue);

    faabric::scheduler::clearMockRequests();
    faabric::util::setMockMode(false);

    // Shutdown the scheduler as we are using the function client/server (even
    // if mocked)
    faabric::scheduler::getScheduler().shutdown();
}

class PlannerClientServerExecTestFixture
  : public PlannerClientServerFixture
  , public FunctionCallClientServerFixture
{
  public:
    PlannerClientServerExecTestFixture()
      : sch(faabric::scheduler::getScheduler())
    {
        sch.shutdown();
        sch.addHostToGlobalSet();

        std::shared_ptr<faabric::executor::ExecutorFactory> fac =
          std::make_shared<faabric::executor::DummyExecutorFactory>();
        faabric::executor::setExecutorFactory(fac);
    }

    ~PlannerClientServerExecTestFixture()
    {
        sch.shutdown();
        sch.addHostToGlobalSet();
    }

  protected:
    faabric::scheduler::Scheduler& sch;
};

TEST_CASE_METHOD(PlannerClientServerExecTestFixture,
                 "Test executing a batch of functions",
                 "[planner]")
{
    int nFuncs = 4;
    faabric::HostResources res;
    res.set_slots(nFuncs);
    sch.setThisHostResources(res);

    auto req = faabric::util::batchExecFactory("foo", "bar", nFuncs);

    auto decision = plannerCli.callFunctions(req);

    for (auto mid : decision.messageIds) {
        auto resultMsg = plannerCli.getMessageResult(decision.appId, mid, 500);
        REQUIRE(resultMsg.returnvalue() == 0);
    }
}

TEST_CASE_METHOD(PlannerClientServerExecTestFixture,
                 "Test getting the scheduling decision from the planner client",
                 "[planner]")
{
    int nFuncs = 4;
    faabric::HostResources res;
    res.set_slots(nFuncs);
    sch.setThisHostResources(res);

    auto req = faabric::util::batchExecFactory("foo", "bar", nFuncs);

    auto decision = plannerCli.callFunctions(req);

    bool appExists;

    SECTION("App not registered")
    {
        appExists = false;
        faabric::util::updateBatchExecAppId(req, 1337);
    }

    SECTION("App registered")
    {
        appExists = true;
    }

    auto actualDecision = plannerCli.getSchedulingDecision(req);

    if (appExists) {
        checkSchedulingDecisionEquality(decision, actualDecision);
    } else {
        faabric::batch_scheduler::SchedulingDecision emptyDecision(0, 0);
        checkSchedulingDecisionEquality(emptyDecision, actualDecision);
    }

    for (auto mid : decision.messageIds) {
        auto resultMsg = plannerCli.getMessageResult(decision.appId, mid, 500);
        REQUIRE(resultMsg.returnvalue() == 0);
    }
}

TEST_CASE_METHOD(PlannerClientServerExecTestFixture,
                 "Test getting the batch results from the planner client",
                 "[planner]")
{
    int nFuncs = 4;
    faabric::HostResources res;
    res.set_slots(nFuncs);
    sch.setThisHostResources(res);
    auto req = faabric::util::batchExecFactory("foo", "bar", nFuncs);

    // If we get the batch results before we call the batch, we get nothing
    auto berStatus = plannerCli.getBatchResults(req);
    REQUIRE(berStatus->appid() == 0);

    // We call and wait for the results
    plannerCli.callFunctions(req);
    std::map<int, Message> messageResults;
    for (int i = 0; i < req->messages_size(); i++) {
        auto result =
          plannerCli.getMessageResult(req->appid(), req->messages(i).id(), 500);
        REQUIRE(result.returnvalue() == 0);
        messageResults[result.id()] = result;
    }

    // Now, all results for the batch should be registered
    berStatus = plannerCli.getBatchResults(req);
    REQUIRE(berStatus->appid() == req->appid());
    for (const auto& msg : berStatus->messageresults()) {
        REQUIRE(messageResults.contains(msg.id()));
        checkMessageEquality(messageResults[msg.id()], msg);
    }
}

TEST_CASE_METHOD(PlannerClientServerExecTestFixture,
                 "Test getting the number of migrations",
                 "[planner]")
{
    // We should be able to get the number of migrations from the planner
    REQUIRE(plannerCli.getNumMigrations() == 0);

    // To see a non-zero result, we need a distributed test
}

TEST_CASE_METHOD(PlannerClientServerExecTestFixture,
                 "Test preloading a scheduling decision from the client",
                 "[planner]")
{
    int nFuncs = 4;
    faabric::HostResources res;
    res.set_slots(nFuncs);
    sch.setThisHostResources(res);
    auto req = faabric::util::batchExecFactory("foo", "bar", nFuncs);

    // Preload a scheduling decision
    auto decision =
      std::make_shared<faabric::batch_scheduler::SchedulingDecision>(
        req->appid(), req->groupid());
    for (int i = 0; i < nFuncs; i++) {
        decision->addMessage(
          faabric::util::getSystemConfig().endpointHost, 0, 0, i);
    }
    plannerCli.preloadSchedulingDecision(decision);

    // Now call the request with a preloaded decision
    plannerCli.callFunctions(req);
    std::map<int, Message> messageResults;
    for (int i = 0; i < req->messages_size(); i++) {
        auto result =
          plannerCli.getMessageResult(req->appid(), req->messages(i).id(), 500);
        REQUIRE(result.returnvalue() == 0);
        messageResults[result.id()] = result;
    }

    // Now, all results for the batch should be registered
    auto berStatus = plannerCli.getBatchResults(req);
    REQUIRE(berStatus->appid() == req->appid());
    for (const auto& msg : berStatus->messageresults()) {
        REQUIRE(messageResults.contains(msg.id()));
        checkMessageEquality(messageResults[msg.id()], msg);
    }
}
}
