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

    REQUIRE(conf.cgroupMode == "on");
    REQUIRE(conf.functionStorage == "local");
    REQUIRE(conf.fileserverUrl == "");
    REQUIRE(conf.netNsMode == "off");
    REQUIRE(conf.logLevel == "info");
    REQUIRE(conf.logFile == "off");
    REQUIRE(conf.pythonPreload == "off");
    REQUIRE(conf.captureStdout == "off");
    REQUIRE(conf.stateMode == "inmemory");
    REQUIRE(conf.wasmVm == "wavm");

    REQUIRE(conf.redisPort == "6379");

    REQUIRE(conf.noScheduler == 0);

    REQUIRE(conf.globalMessageTimeout == 60000);
    REQUIRE(conf.boundTimeout == 30000);
    REQUIRE(conf.unboundTimeout == 300000);
    REQUIRE(conf.chainedCallTimeout == 300000);

    REQUIRE(conf.defaultMpiWorldSize == 5);
}

TEST_CASE("Test overriding system config initialisation", "[util]")
{
    std::string originalHostType = getSystemConfig().hostType;

    std::string hostType = setEnvVar("HOST_TYPE", "magic");
    std::string funcStorage = setEnvVar("FUNCTION_STORAGE", "foobar");
    std::string fileserver = setEnvVar("FILESERVER_URL", "www.foo.com");
    std::string cgMode = setEnvVar("CGROUP_MODE", "off");
    std::string nsMode = setEnvVar("NETNS_MODE", "on");
    std::string logLevel = setEnvVar("LOG_LEVEL", "debug");
    std::string logFile = setEnvVar("LOG_FILE", "on");
    std::string pythonPre = setEnvVar("PYTHON_PRELOAD", "on");
    std::string captureStdout = setEnvVar("CAPTURE_STDOUT", "on");
    std::string stateMode = setEnvVar("STATE_MODE", "foobar");
    std::string wasmVm = setEnvVar("WASM_VM", "blah");

    std::string redisState = setEnvVar("REDIS_STATE_HOST", "not-localhost");
    std::string redisQueue = setEnvVar("REDIS_QUEUE_HOST", "other-host");
    std::string redisPort = setEnvVar("REDIS_PORT", "1234");

    std::string noScheduler = setEnvVar("NO_SCHEDULER", "1");

    std::string globalTimeout = setEnvVar("GLOBAL_MESSAGE_TIMEOUT", "9876");
    std::string boundTimeout = setEnvVar("BOUND_TIMEOUT", "6666");
    std::string unboundTimeout = setEnvVar("UNBOUND_TIMEOUT", "5555");
    std::string chainedTimeout = setEnvVar("CHAINED_CALL_TIMEOUT", "9999");

    std::string faasmLocalDir = setEnvVar("FAASM_LOCAL_DIR", "/tmp/blah");

    std::string mpiSize = setEnvVar("DEFAULT_MPI_WORLD_SIZE", "2468");

    // Create new conf for test
    SystemConfig conf;

    REQUIRE(conf.hostType == "magic");
    REQUIRE(conf.functionStorage == "foobar");
    REQUIRE(conf.fileserverUrl == "www.foo.com");
    REQUIRE(conf.cgroupMode == "off");
    REQUIRE(conf.netNsMode == "on");
    REQUIRE(conf.logLevel == "debug");
    REQUIRE(conf.logFile == "on");
    REQUIRE(conf.pythonPreload == "on");
    REQUIRE(conf.captureStdout == "on");
    REQUIRE(conf.stateMode == "foobar");
    REQUIRE(conf.wasmVm == "blah");

    REQUIRE(conf.redisStateHost == "not-localhost");
    REQUIRE(conf.redisQueueHost == "other-host");
    REQUIRE(conf.redisPort == "1234");

    REQUIRE(conf.noScheduler == 1);

    REQUIRE(conf.globalMessageTimeout == 9876);
    REQUIRE(conf.boundTimeout == 6666);
    REQUIRE(conf.unboundTimeout == 5555);
    REQUIRE(conf.chainedCallTimeout == 9999);

    REQUIRE(conf.functionDir == "/tmp/blah/wasm");
    REQUIRE(conf.objectFileDir == "/tmp/blah/object");
    REQUIRE(conf.runtimeFilesDir == "/tmp/blah/runtime_root");
    REQUIRE(conf.sharedFilesDir == "/tmp/blah/shared");
    REQUIRE(conf.sharedFilesStorageDir == "/tmp/blah/shared_store");

    REQUIRE(conf.defaultMpiWorldSize == 2468);

    // Be careful with host type
    setEnvVar("HOST_TYPE", originalHostType);

    setEnvVar("FUNCTION_STORAGE", funcStorage);
    setEnvVar("FILESERVER_URL", fileserver);
    setEnvVar("CGROUP_MODE", cgMode);
    setEnvVar("NETNS_MODE", nsMode);
    setEnvVar("LOG_LEVEL", logLevel);
    setEnvVar("LOG_FILE", logFile);
    setEnvVar("PYTHON_PRELOAD", pythonPre);
    setEnvVar("CAPTURE_STDOUT", captureStdout);
    setEnvVar("STATE_MODE", stateMode);
    setEnvVar("WASM_VM", wasmVm);

    setEnvVar("REDIS_STATE_HOST", redisState);
    setEnvVar("REDIS_QUEUE_HOST", redisQueue);
    setEnvVar("REDIS_PORT", redisPort);

    setEnvVar("NO_SCHEDULER", noScheduler);

    setEnvVar("GLOBAL_MESSAGE_TIMEOUT", globalTimeout);
    setEnvVar("BOUND_TIMEOUT", boundTimeout);
    setEnvVar("UNBOUND_TIMEOUT", unboundTimeout);
    setEnvVar("CHAINED_CALL_TIMEOUT", chainedTimeout);

    setEnvVar("FAASM_LOCAL_DIR", faasmLocalDir);

    setEnvVar("DEFAULT_MPI_WORLD_SIZE", mpiSize);
}

}
