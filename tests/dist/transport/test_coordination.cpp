#include <catch2/catch.hpp>

#include "dist_test_fixtures.h"
#include "faabric_utils.h"
#include "init.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture, "Test distributed lock", "[ptp][transport]")
{
    // Set up the host resources. The distributed lock test will start 10 other
    // functions (so we need 11 slots). We give each host 8 slots for an even
    // distribution
    int nSlotsPerHost = 8;
    faabric::HostResources res;
    res.set_slots(nSlotsPerHost);
    sch.setThisHostResources(res);
    sch.addHostToGlobalSet(getWorkerIP(), std::make_shared<HostResources>(res));

    // Set up the request
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("ptp", "lock", 1);

    plannerCli.callFunctions(req);

    faabric::Message& m = req->mutable_messages()->at(0);
    faabric::Message result = plannerCli.getMessageResult(m, 30000);
    REQUIRE(result.returnvalue() == 0);
}
}
