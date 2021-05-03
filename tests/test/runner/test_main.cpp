#include "faabric_utils.h"
#include <catch.hpp>

#include <faabric/proto/faabric.pb.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>

using namespace faabric::scheduler;

namespace tests {

TEST_CASE("Test main runner", "[runner]")
{
    cleanFaabric();
    std::shared_ptr<ExecutorFactory> fac =
      faabric::scheduler::getExecutorFactory();
    faabric::runner::FaabricMain m(fac);

    m.startRunner();

    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("foo", "bar", 4);

    auto& sch = faabric::scheduler::getScheduler();
    sch.callFunctions(req);

    for (const auto& m : req->messages()) {
        std::string expected = fmt::format("DummyExecutor executed {}", m.id());
        faabric::Message res = sch.getFunctionResult(m.id(), 1000);
        REQUIRE(res.outputdata() == expected);
    }
}
}
