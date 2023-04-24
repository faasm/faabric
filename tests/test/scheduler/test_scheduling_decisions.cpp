#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>

using namespace faabric::scheduler;

namespace tests {

class SchedulingDecisionTestFixture : public SchedulerTestFixture
{
  public:
    SchedulingDecisionTestFixture()
    {
        faabric::util::setMockMode(true);

        std::shared_ptr<TestExecutorFactory> fac =
          std::make_shared<TestExecutorFactory>();
        setExecutorFactory(fac);
    }

    ~SchedulingDecisionTestFixture() { faabric::util::setMockMode(false); }

  protected:
    std::string masterHost = faabric::util::getSystemConfig().endpointHost;

    // Helper struct to configure one scheduling decision
    struct SchedulingConfig
    {
        std::vector<std::string> hosts;
        std::vector<int> slots;
        std::vector<int> used;
        int numReqs;
        faabric::util::SchedulingTopologyHint topologyHint;
        std::vector<std::string> expectedHosts;
    };

    // We test the scheduling decision twice: the first one will follow the
    // unregistered hosts path, the second one the registerd hosts one.
    void testActualSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const SchedulingConfig& config)
    {
        // Set resources for all hosts
        setHostResources(config.hosts, config.slots, config.used);

        // Set the planner's mocked hosts (hosts that we simulate, but are not
        // online)
        setPlannerMockedHosts(config.hosts);

        auto actualDecision = sch.callFunctions(req);
        REQUIRE(actualDecision.hosts == config.expectedHosts);
    }
};

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test basic scheduling decision",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 1, 1 },
        .used = { 0, 0 },
        .numReqs = 2,
        .topologyHint = faabric::util::SchedulingTopologyHint::NONE,
        // The scheduler breaks ties in reverse alphabetical order, so it will
        // pick "hostA" before "masterHost" (which has a numerical IP)
        .expectedHosts = { "hostA", masterHost },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test scheduling decision on hosts with load",
                 "[scheduler]")
{

    SchedulingConfig config = {
        .hosts = { masterHost, "hostA", "hostB" },
        .slots = { 4, 4, 4 },
        .used = { 0, 0, 0 },
        .topologyHint = faabric::util::SchedulingTopologyHint::NONE,
    };

    // The scheduler picks first hosts with a higher number of available
    // slots
    SECTION("Capacity on all hosts - Emptier hosts go first")
    {
        config.used = { 2, 3, 2 };
        config.numReqs = 5;
        config.expectedHosts = {
            "hostB", "hostB", masterHost, masterHost, "hostA"
        };
    }

    // If all hosts have the same number of available slots, it will pick
    // larger hosts first
    SECTION("Capacity on all hosts - Larger hosts go first")
    {
        config.slots = { 4, 6, 4 };
        config.used = { 2, 4, 2 };
        config.numReqs = 5;
        config.expectedHosts = {
            "hostA", "hostA", "hostB", "hostB", masterHost
        };
    }

    // If all hosts have the same number of available slots and the same size,
    // it will break the tie picking larger hosts alphabetically first
    SECTION("Capacity on all hosts - Larger alphabetically breaks tie")
    {
        config.used = { 2, 2, 2 };
        config.numReqs = 5;
        config.expectedHosts = {
            "hostB", "hostB", "hostA", "hostA", masterHost
        };
    }

    SECTION("One host at capacity")
    {
        config.used = { 2, 4, 2 };
        config.numReqs = 4;
        config.expectedHosts = { "hostB", "hostB", masterHost, masterHost };
    }

    SECTION("Empty scheduling decision if no capacity")
    {
        config.used = { 3, 4, 2 };
        config.numReqs = 5;
        config.expectedHosts = {};
    }

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    testActualSchedulingDecision(req, config);
}

/* TODO: what shall we do with scheduling topology hints?
TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test scheduling hints can be disabled through the config",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 1, 1 },
        .used = { 0, 0 },
        .numReqs = 2,
        .topologyHint = faabric::util::SchedulingTopologyHint::FORCE_LOCAL,
        .expectedHosts = { masterHost, "hostA" },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    auto& faabricConf = faabric::util::getSystemConfig();

    SECTION("Config. variable set")
    {
        faabricConf.noTopologyHints = "on";
        config.expectedHosts = { masterHost, "hostA" };
    }

    SECTION("Config. variable not set")
    {
        config.expectedHosts = { masterHost, masterHost };
    }

    testActualSchedulingDecision(req, config);

    faabricConf.reset();
}
*/

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test master running out of resources",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 0, 2 },
        .used = { 0, 0 },
        .numReqs = 2,
        .topologyHint = faabric::util::SchedulingTopologyHint::NONE,
        .expectedHosts = { "hostA", "hostA" },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test scheduling decision skips fully occupied worker hosts",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA", "hostB" },
        .slots = { 2, 0, 2 },
        .used = { 0, 0, 0 },
        .numReqs = 4,
        .topologyHint = faabric::util::SchedulingTopologyHint::NONE,
        .expectedHosts = { "hostB", "hostB", masterHost, masterHost },
    };

    SECTION("Empty hosts")
    {
        config.slots = { 2, 0, 2 };
        config.used = { 0, 0, 0 };
    }

    SECTION("Full hosts")
    {
        config.slots = { 2, 4, 2 };
        config.used = { 0, 4, 0 };
    }

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test scheduling decision with many requests",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA", "hostB", "hostC" },
        .slots = { 0, 0, 0, 0 },
        .used = { 0, 0, 0, 0 },
        .numReqs = 8,
        .topologyHint = faabric::util::SchedulingTopologyHint::NONE,
        .expectedHosts = { masterHost, masterHost, masterHost, masterHost },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    SECTION("Even slot distribution across hosts")
    {
        config.slots = { 2, 2, 2, 2 };
        config.expectedHosts = { "hostC", "hostC", "hostB",    "hostB",
                                 "hostA", "hostA", masterHost, masterHost };
    }

    SECTION("Uneven slot ditribution across hosts")
    {
        config.slots = { 3, 2, 2, 1 };
        config.expectedHosts = { masterHost, masterHost, masterHost, "hostB",
                                 "hostB",    "hostA",    "hostA",    "hostC" };
    }

    SECTION("Decreasing to one and increasing slot distribution")
    {
        config.slots = { 3, 3, 1, 3 };

        SECTION("No topology hint")
        {
            config.topologyHint = faabric::util::SchedulingTopologyHint::NONE;
            config.expectedHosts = { "hostC", "hostC", "hostC",    "hostA",
                                     "hostA", "hostA", masterHost, masterHost };
        }

        /* TODO: do we want to keep this topology hint?
        SECTION("Never alone topology hint")
        {
            config.topologyHint =
              faabric::util::SchedulingTopologyHint::NEVER_ALONE;
            config.expectedHosts = { masterHost, masterHost, "hostA", "hostA",
                                     "hostC",    "hostC",    "hostC", "hostC" };
        }
        */
    }

    testActualSchedulingDecision(req, config);
}

/* TODO: do we want to keep this test?
TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test sticky pairs scheduling topology hint",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 1, 1 },
        .used = { 0, 0 },
        .numReqs = 2,
        .topologyHint = faabric::util::SchedulingTopologyHint::NEVER_ALONE,
        .expectedHosts = { masterHost, "hostA" },
    };

    std::shared_ptr<faabric::BatchExecuteRequest> req;

    SECTION("Test with hint we only schedule to new hosts pairs of requests")
    {
        config.expectedHosts = { masterHost, masterHost };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test with hint we may overload remote hosts")
    {
        config.hosts = { masterHost, "hostA", "hostB" };
        config.numReqs = 5;
        config.slots = { 2, 2, 1 };
        config.used = { 0, 0, 0 };
        config.expectedHosts = {
            masterHost, masterHost, "hostA", "hostA", "hostA"
        };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION(
      "Test with hint we still overload correctly if running out of slots")
    {
        config.hosts = { masterHost, "hostA" };
        config.numReqs = 5;
        config.slots = { 2, 2 };
        config.used = { 0, 0 };
        config.expectedHosts = {
            masterHost, masterHost, "hostA", "hostA", "hostA"
        };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test hint with uneven slot distribution")
    {
        config.hosts = { masterHost, "hostA" };
        config.numReqs = 5;
        config.slots = { 2, 3 };
        config.used = { 0, 0 };
        config.expectedHosts = {
            masterHost, masterHost, "hostA", "hostA", "hostA"
        };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test hint with uneven slot distribution and overload")
    {
        config.hosts = { masterHost, "hostA", "hostB" };
        config.numReqs = 6;
        config.slots = { 2, 3, 1 };
        config.used = { 0, 0, 0 };
        config.expectedHosts = { masterHost, masterHost, "hostA",
                                 "hostA",    "hostA",    "hostA" };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    testActualSchedulingDecision(req, config);
}
*/

/* TODO: do we want to keep the underfull topology hint?
TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test underfull scheduling topology hint",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 2, 2 },
        .used = { 0, 0 },
        .numReqs = 2,
        .topologyHint = faabric::util::SchedulingTopologyHint::UNDERFULL,
        .expectedHosts = { masterHost, "hostA" },
    };

    std::shared_ptr<faabric::BatchExecuteRequest> req;

    SECTION("Test hint's basic functionality")
    {
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test hint does not affect other hosts")
    {
        config.numReqs = 3;
        config.expectedHosts = { masterHost, "hostA", "hostA" };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test with hint we still overload to master")
    {
        config.numReqs = 4;
        config.expectedHosts = { masterHost, "hostA", "hostA", masterHost };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    testActualSchedulingDecision(req, config);
}
*/

/* TODO: implement CACHED topology hint
TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test cached scheduling topology hint",
                 "[scheduler]")
{
    // Set up a basic scenario
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 2, 2 },
        .used = { 0, 0 },
        .numReqs = 4,
        .topologyHint = faabric::util::SchedulingTopologyHint::CACHED,
        .expectedHosts = { masterHost, masterHost, "hostA", "hostA" },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    int appId = req->messages().at(0).appid();
    testActualSchedulingDecision(req, config);

    // Now change the setup so that another decision would do something
    // different
    SchedulingConfig hitConfig = {
        .hosts = { masterHost, "hostA" },
        .slots = { 0, 10 },
        .used = { 0, 0 },
        .numReqs = 4,
        .topologyHint = faabric::util::SchedulingTopologyHint::CACHED,
        .expectedHosts = { masterHost, masterHost, "hostA", "hostA" },
    };

    // Note we must preserve the app ID to get a cached decision
    auto hitReq = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    for (int i = 0; i < hitReq->messages_size(); i++) {
        hitReq->mutable_messages()->at(i).set_appid(appId);
    }
    testActualSchedulingDecision(hitReq, hitConfig);

    // Change app ID and confirm we get new decision
    SchedulingConfig missConfig = {
        .hosts = { masterHost, "hostA" },
        .slots = { 0, 10 },
        .used = { 0, 0 },
        .numReqs = 4,
        .topologyHint = faabric::util::SchedulingTopologyHint::CACHED,
        .expectedHosts = { "hostA", "hostA", "hostA", "hostA" },
    };
    auto missReq =
      faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    testActualSchedulingDecision(missReq, missConfig);
}
*/
}
