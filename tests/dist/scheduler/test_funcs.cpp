#include "faabric_utils.h"
#include <catch.hpp>

#include "fixtures.h"
#include "init.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture,
                 "Test executing functions on multiple hosts",
                 "[funcs]")
{
    // Set up this host's resources
    int nLocalSlots = 2;
    int nFuncs = 4;
    faabric::HostResources res;
    res.set_slots(nLocalSlots);
    sch.setThisHostResources(res);

    // Set up the messages
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("funcs", "simple", nFuncs);

    // Call the functions
    sch.callFunctions(req);

    // Check functions executed on this host
    for (int i = 0; i < nLocalSlots; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);

        sch.getFunctionResult(m.id(), 1000);
        std::string expected =
          fmt::format("Function {} executed on host {}", m.id(), MASTER_IP);

        REQUIRE(m.outputdata() == expected);
    }

    // Check functions executed on the other host
    for (int i = nLocalSlots; i < nFuncs; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);
        sch.getFunctionResult(m.id(), 1000);

        std::string expected =
          fmt::format("Function {} executed on host {}", m.id(), WORKER_IP);

        REQUIRE(m.outputdata() == expected);
    }
}
}
