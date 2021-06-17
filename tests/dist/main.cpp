#define CATCH_CONFIG_RUNNER

#include "DistTestExecutor.h"
#include "faabric_utils.h"
#include <catch.hpp>

#include "init.h"

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/util/logging.h>

using namespace faabric::scheduler;

struct LogListener : Catch::TestEventListenerBase
{
    // Note, we must call base class versions of overridden methods
    // https://github.com/catchorg/Catch2/pull/1617
    using TestEventListenerBase::TestEventListenerBase;

    void testCaseStarting(Catch::TestCaseInfo const& testInfo) override
    {
        this->Catch::TestEventListenerBase::testCaseStarting(testInfo);

        SPDLOG_INFO("=============================================");
        SPDLOG_INFO("TEST: {}", testInfo.name);
        SPDLOG_INFO("=============================================");
    }

    void sectionStarting(Catch::SectionInfo const& sectionInfo) override
    {
        this->Catch::TestEventListenerBase::sectionStarting(sectionInfo);

        // Tests without any sections will be default have one section with the
        // same name as the test
        if (sectionInfo.name != currentTestCaseInfo->name) {
            SPDLOG_INFO("---------------------------------------------");
            SPDLOG_INFO("SECTION: {}", sectionInfo.name);
            SPDLOG_INFO("---------------------------------------------");
        }
    }
};

CATCH_REGISTER_LISTENER(LogListener)

int main(int argc, char* argv[])
{
    faabric::util::initLogging();

    // Set up the distributed tests
    tests::initDistTests();

    // Start everything up
    SPDLOG_INFO("Starting distributed test server on master");
    std::shared_ptr<ExecutorFactory> fac =
      std::make_shared<tests::DistTestExecutorFactory>();
    faabric::runner::FaabricMain m(fac);
    m.startBackground();

    // Wait for things to start
    usleep(3000 * 1000);

    // Run the tests
    int result = Catch::Session().run(argc, argv);
    fflush(stdout);

    // Shut down
    SPDLOG_INFO("Shutting down");
    m.shutdown();

    return result;
}
