#include <catch.hpp>

#include <faabric/util/config.h>
#include <faabric/util/environment.h>

using namespace faabric::util;

namespace tests {
TEST_CASE("Test default system config initialisation", "[util]")
{
    SystemConfig conf;
    conf.reset();

    REQUIRE(conf.redisStateHost == "redis");
    REQUIRE(conf.redisQueueHost == "redis");

    REQUIRE(conf.logLevel == "info");
    REQUIRE(conf.logFile == "off");
    REQUIRE(conf.stateMode == "inmemory");

    REQUIRE(conf.redisPort == "6379");

    REQUIRE(conf.noScheduler == 0);
    REQUIRE(conf.overrideCpuCount == 0);

    REQUIRE(conf.globalMessageTimeout == 60000);
    REQUIRE(conf.boundTimeout == 30000);

    REQUIRE(conf.defaultMpiWorldSize == 5);
}

TEST_CASE("Test overriding system config initialisation", "[util]")
{
    std::string logLevel = setEnvVar("LOG_LEVEL", "debug");
    std::string logFile = setEnvVar("LOG_FILE", "on");
    std::string pythonPre = setEnvVar("PYTHON_PRELOAD", "on");
    std::string captureStdout = setEnvVar("CAPTURE_STDOUT", "on");
    std::string stateMode = setEnvVar("STATE_MODE", "foobar");

    std::string redisState = setEnvVar("REDIS_STATE_HOST", "not-localhost");
    std::string redisQueue = setEnvVar("REDIS_QUEUE_HOST", "other-host");
    std::string redisPort = setEnvVar("REDIS_PORT", "1234");

    std::string noScheduler = setEnvVar("NO_SCHEDULER", "1");
    std::string overrideCpuCount = setEnvVar("OVERRIDE_CPU_COUNT", "4");

    std::string globalTimeout = setEnvVar("GLOBAL_MESSAGE_TIMEOUT", "9876");
    std::string boundTimeout = setEnvVar("BOUND_TIMEOUT", "6666");

    std::string functionThreads = setEnvVar("FUNCTION_SERVER_THREADS", "111");
    std::string stateThreads = setEnvVar("STATE_SERVER_THREADS", "222");
    std::string snapshotThreads = setEnvVar("SNAPSHOT_SERVER_THREADS", "333");

    std::string mpiSize = setEnvVar("DEFAULT_MPI_WORLD_SIZE", "2468");

    // Create new conf for test
    SystemConfig conf;

    REQUIRE(conf.logLevel == "debug");
    REQUIRE(conf.logFile == "on");
    REQUIRE(conf.stateMode == "foobar");

    REQUIRE(conf.redisStateHost == "not-localhost");
    REQUIRE(conf.redisQueueHost == "other-host");
    REQUIRE(conf.redisPort == "1234");

    REQUIRE(conf.noScheduler == 1);
    REQUIRE(conf.overrideCpuCount == 4);

    REQUIRE(conf.globalMessageTimeout == 9876);
    REQUIRE(conf.boundTimeout == 6666);

    REQUIRE(conf.functionServerThreads == 111);
    REQUIRE(conf.stateServerThreads == 222);
    REQUIRE(conf.snapshotServerThreads == 333);

    REQUIRE(conf.defaultMpiWorldSize == 2468);

    // Be careful with host type
    setEnvVar("LOG_LEVEL", logLevel);
    setEnvVar("LOG_FILE", logFile);
    setEnvVar("PYTHON_PRELOAD", pythonPre);
    setEnvVar("CAPTURE_STDOUT", captureStdout);
    setEnvVar("STATE_MODE", stateMode);

    setEnvVar("REDIS_STATE_HOST", redisState);
    setEnvVar("REDIS_QUEUE_HOST", redisQueue);
    setEnvVar("REDIS_PORT", redisPort);

    setEnvVar("NO_SCHEDULER", noScheduler);
    setEnvVar("OVERRIDE_CPU_COUNT", overrideCpuCount);

    setEnvVar("GLOBAL_MESSAGE_TIMEOUT", globalTimeout);
    setEnvVar("BOUND_TIMEOUT", boundTimeout);

    setEnvVar("FUNCTION_SERVER_THREADS", functionThreads);
    setEnvVar("STATE_SERVER_THREADS", stateThreads);
    setEnvVar("SNAPSHOT_SERVER_THREADS", snapshotThreads);

    setEnvVar("DEFAULT_MPI_WORLD_SIZE", mpiSize);
}

}
