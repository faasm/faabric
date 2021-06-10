#define CATCH_CONFIG_RUNNER

#include <catch.hpp>

#include "faabric_utils.h"

#include <faabric/util/logging.h>
#include <faabric/util/testing.h>

struct LogListener : Catch::TestEventListenerBase
{
    using TestEventListenerBase::TestEventListenerBase;

    void testCaseStarting(Catch::TestCaseInfo const& testInfo) override
    {

        SPDLOG_INFO("---------------------------------------------");
        SPDLOG_INFO("TEST: {}", testInfo.name);
        SPDLOG_INFO("---------------------------------------------");
    }
};

CATCH_REGISTER_LISTENER(LogListener)

int main(int argc, char* argv[])
{
    faabric::util::setTestMode(true);
    faabric::util::initLogging();

    int result = Catch::Session().run(argc, argv);

    fflush(stdout);

    return result;
}
