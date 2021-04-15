#include <catch.hpp>

#include <faabric/util/config.h>
#include <faabric/util/environment.h>

#include <thread>

using namespace faabric::util;

namespace tests {

TEST_CASE("Test default environment variables", "[util]")
{
    std::string key = "JUNK_VAR";

    // Sanity check for null pointer when env var not set
    char const* val = getenv(key.c_str());
    REQUIRE(val == nullptr);

    REQUIRE(getEnvVar(key, "blah") == "blah");
}

TEST_CASE("Test setting environment variables", "[util]")
{
    unsetEnvVar("MY_VAR");

    // Sanity check for empty string when env var set to empty
    char* currentValue = getenv("MY_VAR");
    REQUIRE(currentValue == nullptr);

    REQUIRE(getEnvVar("MY_VAR", "alpha") == "alpha");

    // Check original is returned when resetting
    const std::string original = setEnvVar("MY_VAR", "beta");
    REQUIRE(original == "");

    const std::string original2 = setEnvVar("MY_VAR", "gamma");
    REQUIRE(original2 == "beta");
}

TEST_CASE("Test overriding CPU count", "[util]")
{
    // Unset just in case it is set
    unsetEnvVar("OVERRIDE_CPU_COUNT");

    unsigned int expectedNumCores = std::thread::hardware_concurrency();
    REQUIRE(getUsableCores() == expectedNumCores);

    getSystemConfig().overrideCpuCount = 4;
    REQUIRE(getUsableCores() == 4);
}
}
