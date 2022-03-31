#include <catch2/catch.hpp>

#include "faabric/util/memory.h"
#include "faabric_utils.h"

#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/func.h>

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
                 "Test stale executor reaping"
                 "[scheduler]")
{
    // Set a short bound timeout so executors become stale quickly
    conf.boundTimeout = 10;

    // Cause the scheduler to scale up
    int nMsgs = 10;
    auto req = faabric::util::batchExecFactory("foo", "bar", nMsgs);
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    sch.callFunctions(req);

    // Check executor count
    REQUIRE(sch.getFunctionExecutorCount(firstMsg) == nMsgs);

    // Wait until they become stale
    SLEEP_MS(10 * conf.boundTimeout);

    // Do the reaping
    int actual = sch.reapStaleExecutors();
    REQUIRE(actual == nMsgs);
    REQUIRE(sch.getFunctionExecutorCount(firstMsg) == 0);
}
}
