#include "faabric/proto/faabric.pb.h"
#include "faabric/scheduler/Scheduler.h"
#include "faabric/util/config.h"
#include "faabric/util/func.h"
#include "faabric_utils.h"
#include <catch.hpp>

#include <faabric/executor/DummyExecutorPool.h>

using namespace faabric::executor;

namespace tests {

void executeWithDummyExecutor(std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    cleanFaabric();

    auto& conf = faabric::util::getSystemConfig();
    int boundOriginal = conf.boundTimeout;
    int unboundOriginal = conf.unboundTimeout;
    int threadPoolOriginal = conf.executorThreadPoolSize;

    conf.executorThreadPoolSize = 10;
    conf.boundTimeout = 1000;
    conf.unboundTimeout = 1000;

    DummyExecutorPool pool(4);
    pool.startThreadPool(true);

    auto& sch = faabric::scheduler::getScheduler();
    sch.callFunctions(req);

    pool.shutdown();

    conf.boundTimeout = boundOriginal;
    conf.unboundTimeout = unboundOriginal;
    conf.executorThreadPoolSize = threadPoolOriginal;
}

TEST_CASE("Test executing simple function", "[executor]")
{
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("foo", "bar", 1);
    uint32_t msgId = req->messages().at(0).id();

    executeWithDummyExecutor(req);

    auto& sch = faabric::scheduler::getScheduler();
    faabric::Message result = sch.getFunctionResult(msgId, 1000);
    REQUIRE(result.outputdata() == "Executed by DummyExecutor");
}
}
