#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/redis/Redis.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/macros.h>

using namespace scheduler;

namespace tests {
TEST_CASE("Test execution graph", "[scheduler]")
{
    faabric::Message msgA = faabric::util::messageFactory("demo", "echo");
    faabric::Message msgB1 = faabric::util::messageFactory("demo", "echo");
    faabric::Message msgB2 = faabric::util::messageFactory("demo", "echo");
    faabric::Message msgC1 = faabric::util::messageFactory("demo", "echo");
    faabric::Message msgC2 = faabric::util::messageFactory("demo", "echo");
    faabric::Message msgC3 = faabric::util::messageFactory("demo", "echo");
    faabric::Message msgD = faabric::util::messageFactory("demo", "echo");

    // Set all execution results
    scheduler::Scheduler& sch = scheduler::getScheduler();
    sch.setFunctionResult(msgA);
    sch.setFunctionResult(msgB1);
    sch.setFunctionResult(msgB2);
    sch.setFunctionResult(msgC1);
    sch.setFunctionResult(msgC2);
    sch.setFunctionResult(msgC3);
    sch.setFunctionResult(msgD);

    // Set up chaining relationships
    sch.logChainedFunction(msgA.id(), msgB1.id());
    sch.logChainedFunction(msgA.id(), msgB2.id());
    sch.logChainedFunction(msgB1.id(), msgC1.id());
    sch.logChainedFunction(msgB2.id(), msgC2.id());
    sch.logChainedFunction(msgB2.id(), msgC3.id());
    sch.logChainedFunction(msgC2.id(), msgD.id());

    ExecGraph actual = sch.getFunctionExecGraph(msgA.id());

    ExecGraphNode nodeD = {
        .msg = msgD,
    };

    ExecGraphNode nodeC3 = {
        .msg = msgC3,
    };

    ExecGraphNode nodeC2 = { .msg = msgC2, .children = { nodeD } };

    ExecGraphNode nodeC1 = {
        .msg = msgC1,
    };

    ExecGraphNode nodeB2 = { .msg = msgB2, .children = { nodeC2, nodeC3 } };

    ExecGraphNode nodeB1 = { .msg = msgB1, .children = { nodeC1 } };

    ExecGraphNode nodeA = { .msg = msgA, .children = { nodeB1, nodeB2 } };

    ExecGraph expected{ .rootNode = nodeA };

    // Sense-check nodes in both
    REQUIRE(countExecGraphNodes(actual) == 7);
    REQUIRE(countExecGraphNodes(expected) == 7);

    checkExecGraphEquality(expected, actual);
}

TEST_CASE_METHOD(MpiBaseTestFixture,
                 "Test MPI execution graph",
                 "[mpi][scheduler]")
{
    faabric::scheduler::MpiWorld world;
    msg.set_ismpi(true);

    // Update the result for the master message
    sch.setFunctionResult(msg);

    // Build the message vector to reconstruct the graph
    std::vector<faabric::Message> messages(worldSize);
    for (int rank = 0; rank < worldSize; rank++) {
        messages.at(rank) = faabric::util::messageFactory("mpi", "hellompi");
        messages.at(rank).set_id(0);
        messages.at(rank).set_timestamp(0);
        messages.at(rank).set_finishtimestamp(0);
        messages.at(rank).set_resultkey("");
        messages.at(rank).set_statuskey("");
        messages.at(rank).set_executedhost(
          faabric::util::getSystemConfig().endpointHost);
        messages.at(rank).set_ismpi(true);
        messages.at(rank).set_mpiworldid(worldId);
        messages.at(rank).set_mpirank(rank);
        messages.at(rank).set_mpiworldsize(worldSize);
    }

    world.create(msg, worldId, worldSize);

    world.destroy();

    // Build expected graph
    ExecGraphNode nodeB1 = { .msg = messages.at(1) };
    ExecGraphNode nodeB2 = { .msg = messages.at(2) };
    ExecGraphNode nodeB3 = { .msg = messages.at(3) };
    ExecGraphNode nodeB4 = { .msg = messages.at(4) };

    ExecGraphNode nodeA = { .msg = messages.at(0),
                            .children = { nodeB1, nodeB2, nodeB3, nodeB4 } };

    ExecGraph expected{ .rootNode = nodeA };

    // Wait for the MPI messages to finish
    sch.getFunctionResult(msg.id(), 500);
    for (const auto& id : sch.getChainedFunctions(msg.id())) {
        sch.getFunctionResult(id, 500);
    }
    ExecGraph actual = sch.getFunctionExecGraph(msg.id());

    // Unset the fields that we can't recreate
    actual.rootNode.msg.set_id(0);
    actual.rootNode.msg.set_finishtimestamp(0);
    actual.rootNode.msg.set_timestamp(0);
    actual.rootNode.msg.set_resultkey("");
    actual.rootNode.msg.set_statuskey("");
    actual.rootNode.msg.set_outputdata("");
    for (auto& node : actual.rootNode.children) {
        node.msg.set_id(0);
        node.msg.set_finishtimestamp(0);
        node.msg.set_timestamp(0);
        node.msg.set_resultkey("");
        node.msg.set_statuskey("");
        node.msg.set_outputdata("");
    }

    // Check the execution graph
    REQUIRE(countExecGraphNodes(actual) == worldSize);
    REQUIRE(countExecGraphNodes(expected) == worldSize);

    checkExecGraphEquality(expected, actual);
}
}
