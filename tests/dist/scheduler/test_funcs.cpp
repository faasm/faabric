#include "faabric_utils.h"
#include <catch.hpp>

#include "fixtures.h"
#include "init.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/DistributedCoordinator.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/logging.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture,
                 "Test distributed barrier coordination"
                 "[sync]")
{
    // Set up this host's resources
    int nLocalSlots = 1;
    faabric::HostResources res;
    res.set_slots(nLocalSlots);
    sch.setThisHostResources(res);

    // Set up the messages
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("coord", "barrier", 1);

    // Call the function
    sch.callFunctions(req);
<<<<<<< HEAD
=======

    // Check functions executed on this host
    for (int i = 0; i < nLocalSlots; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);

        sch.getFunctionResult(m.id(), 1000);
        std::string expected =
          fmt::format("Function {} executed on host {}", m.id(), getMasterIP());

        REQUIRE(m.outputdata() == expected);
    }

    // Check functions executed on the other host
    for (int i = nLocalSlots; i < nFuncs; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);
        faabric::Message result = sch.getFunctionResult(m.id(), 1000);

        std::string expected =
          fmt::format("Function {} executed on host {}", m.id(), getWorkerIP());

        REQUIRE(result.outputdata() == expected);
    }
>>>>>>> master
}
}
