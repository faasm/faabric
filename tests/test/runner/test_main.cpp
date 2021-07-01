#include "faabric_utils.h"
#include <catch.hpp>

#include <faabric/proto/faabric.pb.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

using namespace faabric::scheduler;

namespace tests {

class MainRunnerTestFixture : public SchedulerTestFixture
{
  public:
    MainRunnerTestFixture()
    {
        std::shared_ptr<faabric::scheduler::ExecutorFactory> fac =
          std::make_shared<faabric::scheduler::DummyExecutorFactory>();
        faabric::scheduler::setExecutorFactory(fac);
    }
};

TEST_CASE_METHOD(MainRunnerTestFixture, "Test main runner", "[runner]")
{
    std::shared_ptr<ExecutorFactory> fac =
      faabric::scheduler::getExecutorFactory();
    faabric::runner::FaabricMain m(fac);

    m.startBackground();

    SECTION("Do nothing") {}

    SECTION("Make calls")
    {
        std::shared_ptr<faabric::BatchExecuteRequest> req =
          faabric::util::batchExecFactory("foo", "bar", 4);

        auto& sch = faabric::scheduler::getScheduler();
        sch.callFunctions(req);

        for (const auto& m : req->messages()) {
            std::string expected =
              fmt::format("DummyExecutor executed {}", m.id());
            faabric::Message res =
              sch.getFunctionResult(m.id(), SHORT_TEST_TIMEOUT_MS);
            REQUIRE(res.outputdata() == expected);
        }
    }

    m.shutdown();
}

}
