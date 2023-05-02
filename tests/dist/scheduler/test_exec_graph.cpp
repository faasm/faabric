#include <catch2/catch.hpp>

#include "dist_test_fixtures.h"

#include <faabric/scheduler/Scheduler.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture,
                 "Test generating the execution graph",
                 "[funcs]")
{
    // Set up this host's resources
    int nLocalSlots = 2;
    int nFuncs = 4;
    faabric::HostResources res;
    res.set_slots(nLocalSlots);
    sch.setThisHostResources(res);

    // Retry the test a number of times to catch the race-condition where
    // we get the execution graph before all results have been published
    int numRetries = 10;
    for (int r = 0; r < numRetries; r++) {
        // Set up the messages
        std::shared_ptr<faabric::BatchExecuteRequest> req =
          faabric::util::batchExecFactory("funcs", "simple", nFuncs);

        // Add a fictional chaining dependency between functions
        for (int i = 1; i < nFuncs; i++) {
            sch.logChainedFunction(req->mutable_messages()->at(0).id(),
                                   req->mutable_messages()->at(i).id());
        }

        // Call the functions
        sch.callFunctions(req);

        faabric::Message& m = req->mutable_messages()->at(0);

        // Wait for the result, and immediately after query for the execution
        // graph
        faabric::Message result = sch.getFunctionResult(m, 1000);
        auto execGraph = sch.getFunctionExecGraph(m.id());
        REQUIRE(countExecGraphNodes(execGraph) == nFuncs);

        REQUIRE(execGraph.rootNode.msg.id() == m.id());
        for (int i = 1; i < nFuncs; i++) {
            auto node = execGraph.rootNode.children.at(i - 1);
            REQUIRE(node.msg.id() == req->mutable_messages()->at(i).id());
        }
    }
}
}
