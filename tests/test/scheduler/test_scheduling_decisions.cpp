#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/scheduler/Scheduler.h>

using namespace faabric::scheduler;

namespace tests {

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
