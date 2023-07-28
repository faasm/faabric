#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

using namespace faabric::scheduler;

namespace tests {

class MainRunnerTestFixture : public SchedulerFixture
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
        // In the tests, the executing and the waiting threads are in the
        // same address space. We thus need to be careful when calling
        // functions with a ber, and then waiting for the messages in that
        // request. In order to avoid data races, its safe to just use the
        // app id and the message id
        int appId = req->messages(0).appid();
        std::vector<int> msgIds;
        std::for_each(req->mutable_messages()->begin(),
                      req->mutable_messages()->end(),
                      [&msgIds](auto msg) { msgIds.push_back(msg.id()); });

        auto& sch = faabric::scheduler::getScheduler();
        sch.callFunctions(req);

        for (auto msgId : msgIds) {
            std::string expected =
              fmt::format("DummyExecutor executed {}", msgId);
            faabric::Message res =
              faabric::planner::getPlannerClient()->getMessageResult(
                appId, msgId, SHORT_TEST_TIMEOUT_MS);
            REQUIRE(res.outputdata() == expected);
        }
    }

    m.shutdown();
}

}
