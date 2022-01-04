#include <catch2/catch.hpp>

#include <fixtures.h>

#include <faabric/scheduler/FunctionMigrationServer.h>

using namespace faabric::scheduler;

namespace tests {
class FunctionMigrationTestFixture
  : public SchedulerTestFixture
  , public ConfTestFixture
{
  protected:
    FunctionMigrationServer server;

  public:
    FunctionMigrationTestFixture()
    {
        conf.funcMigration = "on";
        conf.migrationCheckPeriod = 2;
    }
};

TEST_CASE_METHOD(FunctionMigrationTestFixture,
                 "Test starting and stopping the function migration server",
                 "[scheduler][migration]")
{
    SECTION("Disable function migration")
    {
        // No-ops the start and stop calls
        conf.funcMigration = "off";
    }

    SECTION("Enable function migration") { conf.funcMigration = "on"; }

    server.start();

    SLEEP_MS(conf.migrationCheckPeriod * 1000);

    server.stop();
}

TEST_CASE_METHOD(
  FunctionMigrationTestFixture,
  "Test can't run function migration server with non-positive check period",
  "[scheduler][migration]")
{
    conf.migrationCheckPeriod = 0;

    REQUIRE_THROWS(server.start());
}
}
