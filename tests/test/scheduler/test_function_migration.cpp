#include <catch2/catch.hpp>

#include <faabric_utils.h>
#include <fixtures.h>

#include <faabric/scheduler/FunctionMigrationThread.h>
#include <faabric/util/testing.h>

using namespace faabric::scheduler;

namespace tests {
class FunctionMigrationTestFixture : public SchedulerTestFixture
{
  public:
    FunctionMigrationTestFixture() { faabric::util::setMockMode(true); }

    ~FunctionMigrationTestFixture() { faabric::util::setMockMode(false); }

  protected:
    FunctionMigrationThread migrationThread;
};

TEST_CASE_METHOD(FunctionMigrationTestFixture,
                 "Test starting and stopping the function migration thread",
                 "[scheduler]")
{
    int wakeUpPeriodSeconds = 2;
    migrationThread.start(wakeUpPeriodSeconds);

    SLEEP_MS(SHORT_TEST_TIMEOUT_MS);

    migrationThread.stop();
}
}
