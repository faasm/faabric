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
    // Set up this host's resources
    int nLocalSlots = 5;
    faabric::HostResources res;
    res.set_slots(nLocalSlots);
    sch.setThisHostResources(res);

    // Set up the request
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("ptp", "lock", 1);

    plannerCli.callFunctions(req);

    faabric::Message& m = req->mutable_messages()->at(0);
    faabric::Message result = plannerCli.getMessageResult(m, 30000);
    REQUIRE(result.returnvalue() == 0);
}
}
