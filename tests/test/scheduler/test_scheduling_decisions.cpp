#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>

using namespace faabric::scheduler;

namespace tests {

// TODO(planner-schedule): remove this file as this is already tested in
// tests/test/batch-scheduler

class SchedulingDecisionTestFixture : public SchedulerFixture
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
    std::string mainHost = faabric::util::getSystemConfig().endpointHost;

    // Helper struct to configure one scheduling decision
    struct SchedulingConfig
    {
        std::vector<std::string> hosts;
        std::vector<int> slots;
        std::vector<int> used;
        int numReqs;
        faabric::batch_scheduler::SchedulingTopologyHint topologyHint;
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

        // The first time we run the batch request, we will follow the
        // unregistered hosts path
        faabric::batch_scheduler::SchedulingDecision actualDecision =
          sch.makeSchedulingDecision(req, config.topologyHint);
        REQUIRE(actualDecision.hosts == config.expectedHosts);

        // Reestablish host resources
        setHostResources(config.hosts, config.slots, config.used);

        // Create a new request, preserving the app ID to ensure a repeat
        // request uses the registered hosts
        auto repeatReq =
          faabric::util::batchExecFactory("foo", "baz", req->messages_size());
        for (int i = 0; i < req->messages_size(); i++) {
            faabric::Message& msg = repeatReq->mutable_messages()->at(i);
            msg.set_appid(actualDecision.appId);
        }

        // The second time we run the batch request, we will follow
        // the registered hosts path
        actualDecision =
          sch.makeSchedulingDecision(repeatReq, config.topologyHint);
        REQUIRE(actualDecision.hosts == config.expectedHosts);
    }
};

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test basic scheduling decision",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { mainHost, "hostA" },
        .slots = { 1, 1 },
        .used = { 0, 0 },
        .numReqs = 2,
        .topologyHint = faabric::batch_scheduler::SchedulingTopologyHint::NONE,
        .expectedHosts = { mainHost, "hostA" },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test scheduling decision on hosts with load",
                 "[scheduler]")
{

    SchedulingConfig config = {
        .hosts = { mainHost, "hostA", "hostB" },
        .slots = { 4, 4, 4 },
        .used = { 0, 0, 0 },
        .topologyHint = faabric::batch_scheduler::SchedulingTopologyHint::NONE,
    };

    SECTION("Capacity on all hosts")
    {
        config.used = { 2, 3, 2 };
        config.numReqs = 5;
        config.expectedHosts = {
            mainHost, mainHost, "hostA", "hostB", "hostB"
        };
    }

    SECTION("Non-main host overloaded")
    {
        config.used = { 2, 6, 2 };
        config.numReqs = 4;
        config.expectedHosts = { mainHost, mainHost, "hostB", "hostB" };
    }

    SECTION("Non-main host overloaded, insufficient capacity")
    {
        config.used = { 3, 6, 2 };
        config.numReqs = 5;
        config.expectedHosts = {
            mainHost, "hostB", "hostB", mainHost, mainHost,
        };
    }

    SECTION("Non-main host overloaded, main overloaded")
    {
        config.used = { 6, 6, 2 };
        config.numReqs = 5;
        config.expectedHosts = {
            "hostB", "hostB", mainHost, mainHost, mainHost,
        };
    }

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test overloading all resources defaults to main",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { mainHost, "hostA" },
        .slots = { 1, 1 },
        .used = { 0, 0 },
        .numReqs = 3,
        .topologyHint = faabric::batch_scheduler::SchedulingTopologyHint::NONE,
        .expectedHosts = { mainHost, "hostA", mainHost },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test force local forces executing at main",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { mainHost, "hostA" },
        .slots = { 1, 1 },
        .used = { 0, 0 },
        .numReqs = 2,
        .topologyHint =
          faabric::batch_scheduler::SchedulingTopologyHint::FORCE_LOCAL,
        .expectedHosts = { mainHost, "hostA" },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    SECTION("Force local off")
    {
        config.topologyHint =
          faabric::batch_scheduler::SchedulingTopologyHint::NONE,
        config.expectedHosts = { mainHost, "hostA" };
    }

    SECTION("Force local on")
    {
        config.topologyHint =
          faabric::batch_scheduler::SchedulingTopologyHint::FORCE_LOCAL,
        config.expectedHosts = { mainHost, mainHost };
    }

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test scheduling hints can be disabled through the config",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { mainHost, "hostA" },
        .slots = { 1, 1 },
        .used = { 0, 0 },
        .numReqs = 2,
        .topologyHint =
          faabric::batch_scheduler::SchedulingTopologyHint::FORCE_LOCAL,
        .expectedHosts = { mainHost, "hostA" },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    auto& faabricConf = faabric::util::getSystemConfig();

    SECTION("Config. variable set")
    {
        faabricConf.noTopologyHints = "on";
        config.expectedHosts = { mainHost, "hostA" };
    }

    SECTION("Config. variable not set")
    {
        config.expectedHosts = { mainHost, mainHost };
    }

    testActualSchedulingDecision(req, config);

    faabricConf.reset();
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test main running out of resources",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { mainHost, "hostA" },
        .slots = { 0, 2 },
        .used = { 0, 0 },
        .numReqs = 2,
        .topologyHint = faabric::batch_scheduler::SchedulingTopologyHint::NONE,
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
        .hosts = { mainHost, "hostA", "hostB" },
        .slots = { 2, 0, 2 },
        .used = { 0, 0, 0 },
        .numReqs = 4,
        .topologyHint = faabric::batch_scheduler::SchedulingTopologyHint::NONE,
        .expectedHosts = { mainHost, mainHost, "hostB", "hostB" },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    SECTION("No topology hint")
    {
        config.topologyHint =
          faabric::batch_scheduler::SchedulingTopologyHint::NONE;
    }

    SECTION("Never alone topology hint")
    {
        config.topologyHint =
          faabric::batch_scheduler::SchedulingTopologyHint::NEVER_ALONE;
    }

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test scheduling decision with many requests",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { mainHost, "hostA", "hostB", "hostC" },
        .slots = { 0, 0, 0, 0 },
        .used = { 0, 0, 0, 0 },
        .numReqs = 8,
        .topologyHint = faabric::batch_scheduler::SchedulingTopologyHint::NONE,
        .expectedHosts = { mainHost, mainHost, mainHost, mainHost },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);

    SECTION("Even slot distribution across hosts")
    {
        config.slots = { 2, 2, 2, 2 };
        config.expectedHosts = { mainHost, mainHost, "hostA", "hostA",
                                 "hostB",  "hostB",  "hostC", "hostC" };
    }

    SECTION("Uneven slot ditribution across hosts")
    {
        config.slots = { 3, 2, 2, 1 };
        config.expectedHosts = { mainHost, mainHost, mainHost, "hostA",
                                 "hostA",  "hostB",  "hostB",  "hostC" };
    }

    SECTION("Very uneven slot distribution across hosts")
    {
        config.slots = { 1, 0, 0, 0 };
        config.expectedHosts = { mainHost, mainHost, mainHost, mainHost,
                                 mainHost, mainHost, mainHost, mainHost };
    }

    SECTION("Decreasing to one and increasing slot distribution")
    {
        config.slots = { 2, 2, 1, 2 };

        SECTION("No topology hint")
        {
            config.topologyHint =
              faabric::batch_scheduler::SchedulingTopologyHint::NONE;
            config.expectedHosts = { mainHost, mainHost, "hostA", "hostA",
                                     "hostB",  "hostC",  "hostC", mainHost };
        }

        SECTION("Never alone topology hint")
        {
            config.topologyHint =
              faabric::batch_scheduler::SchedulingTopologyHint::NEVER_ALONE;
            config.expectedHosts = { mainHost, mainHost, "hostA", "hostA",
                                     "hostC",  "hostC",  "hostC", "hostC" };
        }
    }

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test sticky pairs scheduling topology hint",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { mainHost, "hostA" },
        .slots = { 1, 1 },
        .used = { 0, 0 },
        .numReqs = 2,
        .topologyHint =
          faabric::batch_scheduler::SchedulingTopologyHint::NEVER_ALONE,
        .expectedHosts = { mainHost, "hostA" },
    };

    std::shared_ptr<faabric::BatchExecuteRequest> req;

    SECTION("Test with hint we only schedule to new hosts pairs of requests")
    {
        config.expectedHosts = { mainHost, mainHost };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test with hint we may overload remote hosts")
    {
        config.hosts = { mainHost, "hostA", "hostB" };
        config.numReqs = 5;
        config.slots = { 2, 2, 1 };
        config.used = { 0, 0, 0 };
        config.expectedHosts = {
            mainHost, mainHost, "hostA", "hostA", "hostA"
        };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION(
      "Test with hint we still overload correctly if running out of slots")
    {
        config.hosts = { mainHost, "hostA" };
        config.numReqs = 5;
        config.slots = { 2, 2 };
        config.used = { 0, 0 };
        config.expectedHosts = {
            mainHost, mainHost, "hostA", "hostA", "hostA"
        };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test hint with uneven slot distribution")
    {
        config.hosts = { mainHost, "hostA" };
        config.numReqs = 5;
        config.slots = { 2, 3 };
        config.used = { 0, 0 };
        config.expectedHosts = {
            mainHost, mainHost, "hostA", "hostA", "hostA"
        };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test hint with uneven slot distribution and overload")
    {
        config.hosts = { mainHost, "hostA", "hostB" };
        config.numReqs = 6;
        config.slots = { 2, 3, 1 };
        config.used = { 0, 0, 0 };
        config.expectedHosts = { mainHost, mainHost, "hostA",
                                 "hostA",  "hostA",  "hostA" };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test underfull scheduling topology hint",
                 "[scheduler]")
{
    SchedulingConfig config = {
        .hosts = { mainHost, "hostA" },
        .slots = { 2, 2 },
        .used = { 0, 0 },
        .numReqs = 2,
        .topologyHint =
          faabric::batch_scheduler::SchedulingTopologyHint::UNDERFULL,
        .expectedHosts = { mainHost, "hostA" },
    };

    std::shared_ptr<faabric::BatchExecuteRequest> req;

    SECTION("Test hint's basic functionality")
    {
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test hint does not affect other hosts")
    {
        config.numReqs = 3;
        config.expectedHosts = { mainHost, "hostA", "hostA" };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test with hint we still overload to main")
    {
        config.numReqs = 4;
        config.expectedHosts = { mainHost, "hostA", "hostA", mainHost };
        req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    }

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test cached scheduling topology hint",
                 "[scheduler]")
{
    // Set up a basic scenario
    SchedulingConfig config = {
        .hosts = { mainHost, "hostA" },
        .slots = { 2, 2 },
        .used = { 0, 0 },
        .numReqs = 4,
        .topologyHint =
          faabric::batch_scheduler::SchedulingTopologyHint::CACHED,
        .expectedHosts = { mainHost, mainHost, "hostA", "hostA" },
    };

    auto req = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    int appId = req->messages().at(0).appid();
    testActualSchedulingDecision(req, config);

    // Now change the setup so that another decision would do something
    // different
    SchedulingConfig hitConfig = {
        .hosts = { mainHost, "hostA" },
        .slots = { 0, 10 },
        .used = { 0, 0 },
        .numReqs = 4,
        .topologyHint =
          faabric::batch_scheduler::SchedulingTopologyHint::CACHED,
        .expectedHosts = { mainHost, mainHost, "hostA", "hostA" },
    };

    // Note we must preserve the app ID to get a cached decision
    auto hitReq = faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    for (int i = 0; i < hitReq->messages_size(); i++) {
        hitReq->mutable_messages()->at(i).set_appid(appId);
    }
    testActualSchedulingDecision(hitReq, hitConfig);

    // Change app ID and confirm we get new decision
    SchedulingConfig missConfig = {
        .hosts = { mainHost, "hostA" },
        .slots = { 0, 10 },
        .used = { 0, 0 },
        .numReqs = 4,
        .topologyHint =
          faabric::batch_scheduler::SchedulingTopologyHint::CACHED,
        .expectedHosts = { "hostA", "hostA", "hostA", "hostA" },
    };
    auto missReq =
      faabric::util::batchExecFactory("foo", "bar", config.numReqs);
    testActualSchedulingDecision(missReq, missConfig);
}
}
