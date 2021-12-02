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

    void checkRecordedBatchMessages(
      faabric::util::SchedulingDecision actualDecision,
      const SchedulingConfig& config)
    {
        auto batchMessages = faabric::scheduler::getBatchRequests();

        // First, turn our expected list of hosts to a map with frequency count
        // and exclude the master host as no message is sent
        std::map<std::string, int> expectedHostCount;
        for (const auto& h : config.expectedHosts) {
            if (h != masterHost) {
                ++expectedHostCount[h];
            }
        }

        // Then check that the count matches the size of the batch sent
        for (const auto& hostReqPair : batchMessages) {
            REQUIRE(expectedHostCount.contains(hostReqPair.first));
            REQUIRE(expectedHostCount.at(hostReqPair.first) ==
                    hostReqPair.second->messages_size());
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
        actualDecision = sch.callFunctions(req, config.topologyHint);
        REQUIRE(actualDecision.hosts == config.expectedHosts);
        checkRecordedBatchMessages(actualDecision, config);

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
        actualDecision = sch.callFunctions(reqCopy, config.topologyHint);
        REQUIRE(actualDecision.hosts == config.expectedHosts);
        checkRecordedBatchMessages(actualDecision, config);
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
        .topologyHint = faabric::util::SchedulingTopologyHint::FORCE_LOCAL,
        .expectedHosts = { masterHost, "hostA" },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    SECTION("Force local off")
    {
        config.topologyHint = faabric::util::SchedulingTopologyHint::NORMAL,
        config.expectedHosts = { masterHost, "hostA" };
    }

    SECTION("Force local on")
    {
        config.topologyHint =
          faabric::util::SchedulingTopologyHint::FORCE_LOCAL,
        config.expectedHosts = { masterHost, masterHost };
    }

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test scheduling hints can be disabled through the config",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 1, 1 },
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

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test master running out of resources",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 0, 2 },
        .numReqs = 2,
        .topologyHint = faabric::util::SchedulingTopologyHint::NORMAL,
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
        .numReqs = 4,
        .topologyHint = faabric::util::SchedulingTopologyHint::NORMAL,
        .expectedHosts = { masterHost, masterHost, "hostB", "hostB" },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    SECTION("No topology hint")
    {
        config.topologyHint = faabric::util::SchedulingTopologyHint::NORMAL;
    }

    SECTION("Never alone topology hint")
    {
        config.topologyHint =
          faabric::util::SchedulingTopologyHint::NEVER_ALONE;
    }

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

    SECTION("Decreasing to one and increasing slot distribution")
    {
        config.slots = { 2, 2, 1, 2 };

        SECTION("No topology hint")
        {
            config.topologyHint = faabric::util::SchedulingTopologyHint::NORMAL;
            config.expectedHosts = {
                masterHost, masterHost, "hostA", "hostA",
                "hostB",    "hostC",    "hostC", masterHost
            };
        }

        SECTION("Never alone topology hint")
        {
            config.topologyHint =
              faabric::util::SchedulingTopologyHint::NEVER_ALONE;
            config.expectedHosts = { masterHost, masterHost, "hostA", "hostA",
                                     "hostC",    "hostC",    "hostC", "hostC" };
        }
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
