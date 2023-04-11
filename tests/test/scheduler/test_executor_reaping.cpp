#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/func.h>
#include <faabric/util/memory.h>

using namespace faabric::scheduler;

namespace tests {

class SchedulerReapingTestFixture
  : public SchedulerTestFixture
  , public ConfTestFixture
{
  public:
    SchedulerReapingTestFixture()
    {
        std::shared_ptr<faabric::scheduler::ExecutorFactory> fac =
          std::make_shared<faabric::scheduler::DummyExecutorFactory>();
        faabric::scheduler::setExecutorFactory(fac);
    }

    ~SchedulerReapingTestFixture() {}
};

TEST_CASE_METHOD(SchedulerReapingTestFixture,
                 "Test stale executor reaping",
                 "[scheduler]")
{
    int boundTimeoutMs = 0;
    int sleepTimeMs = 0;
    bool expectReaped = false;

    // Check nothing happens by default
    int actualBefore = sch.reapStaleExecutors();
    REQUIRE(actualBefore == 0);

    SECTION("When executors are stale")
    {
        boundTimeoutMs = 10;
        sleepTimeMs = 1000;
        expectReaped = true;
    }

    SECTION("When executors aren't stale")
    {
        boundTimeoutMs = 2000;
        sleepTimeMs = 100;
        expectReaped = false;
    }

    // Set a short bound timeout so executors become stale quickly
    conf.boundTimeout = boundTimeoutMs;

    // Cause the scheduler to scale up
    int nMsgs = 10;
    auto req = faabric::util::batchExecFactory("foo", "bar", nMsgs);
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    sch.callFunctions(req);

    // Check executor count
    REQUIRE(sch.getFunctionExecutorCount(firstMsg) == nMsgs);

    // Wait until they become stale
    SLEEP_MS(sleepTimeMs);

    // Do the reaping
    int actual = sch.reapStaleExecutors();

    if (expectReaped) {
        REQUIRE(actual == nMsgs);
        REQUIRE(sch.getFunctionExecutorCount(firstMsg) == 0);
    } else {
        REQUIRE(actual == 0);
        REQUIRE(sch.getFunctionExecutorCount(firstMsg) == nMsgs);
    }

    // Run again, check same result
    int actualTwo = sch.reapStaleExecutors();
    REQUIRE(actualTwo == 0);
    if (expectReaped) {
        REQUIRE(sch.getFunctionExecutorCount(firstMsg) == 0);
    } else {
        REQUIRE(sch.getFunctionExecutorCount(firstMsg) == nMsgs);
    }
}
}
