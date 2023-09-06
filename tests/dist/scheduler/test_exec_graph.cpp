#include <catch2/catch.hpp>

#include "dist_test_fixtures.h"

#include <faabric/scheduler/Scheduler.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture,
                 "Test generating the execution graph",
                 "[funcs]")
{
    // Retry the test a number of times to catch the race-condition where
    // we get the execution graph before all results have been published
    int numRetries = 10;
    for (int r = 0; r < numRetries; r++) {
        // Set up both host's resources
        int nLocalSlots = 2;
        int nFuncs = 4;
        faabric::HostResources res;
        res.set_slots(nLocalSlots);
        sch.setThisHostResources(res);
        sch.addHostToGlobalSet(getWorkerIP(),
                               std::make_shared<HostResources>(res));

        // Set up the messages
        std::shared_ptr<faabric::BatchExecuteRequest> req =
          faabric::util::batchExecFactory("funcs", "simple", nFuncs);

        // Add a fictional chaining dependency between functions
        int appId = req->messages(0).appid();
        std::vector<int> msgIds = { req->messages(0).id() };
        for (int i = 1; i < nFuncs; i++) {
            faabric::util::logChainedFunction(req->mutable_messages()->at(0),
                                              req->mutable_messages()->at(i));
            msgIds.push_back(req->messages(i).id());
        }

        // Call the functions
        plannerCli.callFunctions(req);

        faabric::Message& m = req->mutable_messages()->at(0);

        // Wait for the result, and immediately after query for the execution
        // graph
        for (const auto msgId : msgIds) {
            plannerCli.getMessageResult(appId, msgId, 1000);
        }
        auto execGraph = faabric::util::getFunctionExecGraph(m);
        REQUIRE(countExecGraphNodes(execGraph) == nFuncs);

        REQUIRE(execGraph.rootNode.msg.id() == m.id());
        for (int i = 1; i < nFuncs; i++) {
            auto node = execGraph.rootNode.children.at(i - 1);
            REQUIRE(node.msg.id() == req->mutable_messages()->at(i).id());
        }
    }
}
}
