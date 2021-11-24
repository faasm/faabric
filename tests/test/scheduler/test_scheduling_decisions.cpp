#include <catch2/catch.hpp>

#include "fixtures.h"

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
    int appId = 123;
    int groupId = 456;
    std::string masterHost = faabric::util::getSystemConfig().endpointHost;

    // Helper struct to configure one scheduling decision
    struct SchedulingConfig
    {
        std::vector<std::string> hosts;
        std::vector<int> slots;
        int numReqs;
        bool forceLocal;
        faabric::util::SchedulingTopologyHint topologyHint;
        std::vector<std::string> expectedHosts;
    };

    // Helper method to set the available hosts and slots per host prior to
    // making a scheduling decision
    void setHostResources(std::vector<std::string> registeredHosts,
                          std::vector<int> slotsPerHost)
    {
        assert(registeredHosts.size() == slotsPerHost.size());
        auto& sch = getScheduler();
        sch.clearRecordedMessages();

        for (int i = 0; i < registeredHosts.size(); i++) {
            faabric::HostResources resources;
            resources.set_slots(slotsPerHost.at(i));
            resources.set_usedslots(0);

            sch.addHostToGlobalSet(registeredHosts.at(i));

            // If setting resources for the master host, update the scheduler.
            // Otherwise, queue the resource response
            if (i == 0) {
                sch.setThisHostResources(resources);
            } else {
                queueResourceResponse(registeredHosts.at(i), resources);
            }
        }
    }

    // We test the scheduling decision twice: the first one will follow the
    // unregistered hosts path, the second one the registerd hosts one.
    void testActualSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const SchedulingConfig& config)
    {
        faabric::util::SchedulingDecision actualDecision(appId, groupId);

        // Set resources for all hosts
        setHostResources(config.hosts, config.slots);

        // The first time we run the batch request, we will follow the
        // unregistered hosts path
        actualDecision =
          sch.callFunctions(req, config.forceLocal, config.topologyHint);
        REQUIRE(actualDecision.hosts == config.expectedHosts);

        // We wait for the execution to finish and the scheduler to vacate
        // the slots. We can't wait on the function result, as sometimes
        // functions won't be executed at all (e.g. master running out of
        // resources).
        SLEEP_MS(100);

        // Set resources again to reset the used slots
        auto reqCopy =
          faabric::util::batchExecFactory("foo", "baz", req->messages_size());
        setHostResources(config.hosts, config.slots);

        // The second time we run the batch request, we will follow
        // the registered hosts path
        actualDecision =
          sch.callFunctions(reqCopy, config.forceLocal, config.topologyHint);
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
        .numReqs = 2,
        .forceLocal = false,
        .topologyHint = faabric::util::SchedulingTopologyHint::NORMAL,
        .expectedHosts = { masterHost, "hostA" },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test overloading all resources defaults to master",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 1, 1 },
        .numReqs = 3,
        .forceLocal = false,
        .topologyHint = faabric::util::SchedulingTopologyHint::NORMAL,
        .expectedHosts = { masterHost, "hostA", masterHost },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test force local forces executing at master",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 1, 1 },
        .numReqs = 2,
        .forceLocal = false,
        .topologyHint = faabric::util::SchedulingTopologyHint::NORMAL,
        .expectedHosts = { masterHost, "hostA" },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    SECTION("Force local off")
    {
        config.forceLocal = false;
        config.expectedHosts = { masterHost, "hostA" };
    }

    SECTION("Force local on")
    {
        config.forceLocal = true;
        config.expectedHosts = { masterHost, masterHost };
    }

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test master running out of resources",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 0, 2 },
        .numReqs = 2,
        .forceLocal = false,
        .topologyHint = faabric::util::SchedulingTopologyHint::NORMAL,
        .expectedHosts = { "hostA", "hostA" },
    };

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
        .numReqs = 8,
        .forceLocal = false,
        .topologyHint = faabric::util::SchedulingTopologyHint::NORMAL,
        .expectedHosts = { masterHost, masterHost, masterHost, masterHost },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    SECTION("Even slot distribution across hosts")
    {
        config.slots = { 2, 2, 2, 2 };
        config.expectedHosts = { masterHost, masterHost, "hostA", "hostA",
                                 "hostB",    "hostB",    "hostC", "hostC" };
    }

    SECTION("Uneven slot ditribution across hosts")
    {
        config.slots = { 3, 2, 2, 1 };
        config.expectedHosts = { masterHost, masterHost, masterHost, "hostA",
                                 "hostA",    "hostB",    "hostB",    "hostC" };
    }

    SECTION("Very uneven slot distribution across hosts")
    {
        config.slots = { 1, 0, 0, 0 };
        config.expectedHosts = {
            masterHost, masterHost, masterHost, masterHost,
            masterHost, masterHost, masterHost, masterHost
        };
    }

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test sticky pairs scheduling topology hint",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 1, 1 },
        .numReqs = 2,
        .forceLocal = false,
        .topologyHint = faabric::util::SchedulingTopologyHint::PAIRS,
        .expectedHosts = { masterHost, "hostA" },
    };

    std::shared_ptr<faabric::BatchExecuteRequest> req;

    SECTION("Test with hint we only schedule to new hosts pairs of requests")
    {
        config.expectedHosts = { masterHost, masterHost };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test hint does not apply for master requests")
    {
        config.slots = { 0, 1 };
        config.numReqs = 1;
        config.expectedHosts = { "hostA" };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test with hint we may overload remote hosts")
    {
        config.hosts = { masterHost, "hostA", "hostB" };
        config.numReqs = 5;
        config.slots = { 2, 2, 1 };
        config.expectedHosts = {
            masterHost, masterHost, "hostA", "hostA", "hostA"
        };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test with hint we still overload master if running out of slots")
    {
        config.hosts = { masterHost, "hostA" };
        config.numReqs = 5;
        config.slots = { 2, 2 };
        config.expectedHosts = {
            masterHost, masterHost, "hostA", "hostA", masterHost
        };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test hint with uneven slot distribution")
    {
        config.hosts = { masterHost, "hostA" };
        config.numReqs = 5;
        config.slots = { 2, 3 };
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
        config.expectedHosts = { masterHost, masterHost, "hostA",
                                 "hostA",    "hostA",    "hostA" };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    testActualSchedulingDecision(req, config);
}
}
