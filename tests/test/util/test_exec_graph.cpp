#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/mpi/MpiWorld.h>
#include <faabric/planner/Planner.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/ExecGraph.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/macros.h>

using namespace faabric::util;

namespace tests {
class ExecGraphTestFixture
  : public FunctionCallClientServerFixture
  , public SchedulerFixture
{};

TEST_CASE_METHOD(ExecGraphTestFixture, "Test execution graph", "[util]")
{
    auto ber = faabric::util::batchExecFactory("demo", "echo", 7);
    faabric::Message msgA = *ber->mutable_messages(0);
    faabric::Message msgB1 = *ber->mutable_messages(1);
    faabric::Message msgB2 = *ber->mutable_messages(2);
    faabric::Message msgC1 = *ber->mutable_messages(3);
    faabric::Message msgC2 = *ber->mutable_messages(4);
    faabric::Message msgC3 = *ber->mutable_messages(5);
    faabric::Message msgD = *ber->mutable_messages(6);

    // Set up chaining relationships
    logChainedFunction(msgA, msgB1);
    logChainedFunction(msgA, msgB2);
    logChainedFunction(msgB1, msgC1);
    logChainedFunction(msgB2, msgC2);
    logChainedFunction(msgB2, msgC3);
    logChainedFunction(msgC2, msgD);

    // Set all execution results
    scheduler::Scheduler& sch = scheduler::getScheduler();
    sch.setFunctionResult(msgA);
    sch.setFunctionResult(msgB1);
    sch.setFunctionResult(msgB2);
    sch.setFunctionResult(msgC1);
    sch.setFunctionResult(msgC2);
    sch.setFunctionResult(msgC3);
    sch.setFunctionResult(msgD);

    ExecGraph actual = getFunctionExecGraph(msgA);

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

TEST_CASE_METHOD(ExecGraphTestFixture,
                 "Test can't get exec graph if results are not published",
                 "[util]")
{
    faabric::Message msg = faabric::util::messageFactory("demo", "echo");

    REQUIRE(getFunctionExecGraph(msg).rootNode.msg.id() == 0);
}

TEST_CASE_METHOD(ExecGraphTestFixture,
                 "Test get unique hosts from exec graph",
                 "[scheduler][exec-graph]")
{
    auto ber = faabric::util::batchExecFactory("demo", "echo", 3);
    faabric::Message msgA = *ber->mutable_messages(0);
    faabric::Message msgB1 = *ber->mutable_messages(1);
    faabric::Message msgB2 = *ber->mutable_messages(2);

    msgA.set_executedhost("foo");
    msgB1.set_executedhost("bar");
    msgB2.set_executedhost("baz");

    ExecGraphNode nodeB1 = { .msg = msgB1 };
    ExecGraphNode nodeB2 = { .msg = msgB2 };
    ExecGraphNode nodeB3 = { .msg = msgB2 };
    ExecGraphNode nodeA = { .msg = msgA,
                            .children = { nodeB1, nodeB2, nodeB3 } };

    ExecGraph graph{ .rootNode = nodeA };
    std::set<std::string> expected = { "bar", "baz", "foo" };
    auto hosts = getExecGraphHosts(graph);
    REQUIRE(hosts == expected);
}

TEST_CASE_METHOD(MpiBaseTestFixture, "Test MPI execution graph", "[scheduler]")
{
    faabric::mpi::MpiWorld world;
    msg.set_ismpi(true);
    msg.set_recordexecgraph(true);

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
        messages.at(rank).set_recordexecgraph(true);
    }

    // First call the original message
    plannerCli.callFunctions(req);

    world.create(msg, worldId, worldSize);

    // Build expected graph
    ExecGraphNode nodeB1 = { .msg = messages.at(1) };
    ExecGraphNode nodeB2 = { .msg = messages.at(2) };
    ExecGraphNode nodeB3 = { .msg = messages.at(3) };
    ExecGraphNode nodeB4 = { .msg = messages.at(4) };

    ExecGraphNode nodeA = { .msg = messages.at(0),
                            .children = { nodeB1, nodeB2, nodeB3, nodeB4 } };

    ExecGraph expected{ .rootNode = nodeA };

    // The MPI base fixture uses the DummyExecutor, which immediately sets
    // the function result. We want to overwrite said function result with the
    // chained calls (logged as part of MpiWorld::create) thus we sleep enough
    // to let the dummy executor set the result, to make sure we can overwrite
    // it here
    SLEEP_MS(500);
    sch.setFunctionResult(msg);

    // Wait for the MPI messages to finish
    plannerCli.getMessageResult(msg, 2000);
    for (const auto& id : faabric::util::getChainedFunctions(msg)) {
        plannerCli.getMessageResult(msg.appid(), id, 2000);
    }
    ExecGraph actual = getFunctionExecGraph(msg);

    world.destroy();

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

TEST_CASE_METHOD(ExecGraphTestFixture,
                 "Test exec graph details",
                 "[util][exec-graph]")
{
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    std::string expectedKey = "foo";
    std::string expectedStringValue = "bar";
    int expectedIntValue = 1;

    // By default, recording is disabled
    REQUIRE(msg.recordexecgraph() == false);

    // If we add a recording while disabled, nothing changes
    incrementCounter(msg, expectedKey, expectedIntValue);
    addDetail(msg, expectedKey, expectedStringValue);
    REQUIRE(msg.intexecgraphdetails_size() == 0);
    REQUIRE(msg.execgraphdetails_size() == 0);

    // We can turn it on
    msg.set_recordexecgraph(true);

    // We can add records either to a string or to an int map
    incrementCounter(msg, expectedKey, expectedIntValue);
    addDetail(msg, expectedKey, expectedStringValue);

    // Both change the behaviour of the underlying message
    REQUIRE(msg.intexecgraphdetails_size() == 1);
    REQUIRE(msg.execgraphdetails_size() == 1);
    REQUIRE(msg.intexecgraphdetails().count(expectedKey) == 1);
    REQUIRE(msg.intexecgraphdetails().at(expectedKey) == expectedIntValue);
    REQUIRE(msg.execgraphdetails().count(expectedKey) == 1);
    REQUIRE(msg.execgraphdetails().at(expectedKey) == expectedStringValue);
}
}
