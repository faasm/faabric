#include <catch.hpp>

#include "faabric_utils.h"

#include <faabric/util/logging.h>

namespace tests {
void checkExecGraphNodeEquality(const scheduler::ExecGraphNode& nodeA,
                                const scheduler::ExecGraphNode& nodeB,
                                bool isMpi)
{
    // Check the message itself
    if (isMpi) {
        REQUIRE(nodeA.msg.mpirank() == nodeB.msg.mpirank());
    } else {
        checkMessageEquality(nodeA.msg, nodeB.msg);
    }

    if (nodeA.children.size() != nodeB.children.size()) {
        FAIL(fmt::format("Children not same size: {} vs {}",
                         nodeA.children.size(),
                         nodeB.children.size()));
    }

    // Assume children are in same order
    for (int i = 0; i < nodeA.children.size(); i++) {
        checkExecGraphNodeEquality(
          nodeA.children.at(i), nodeB.children.at(i), isMpi);
    }
}

void checkExecGraphEquality(const scheduler::ExecGraph& graphA,
                            const scheduler::ExecGraph& graphB,
                            bool isMpi)
{
    checkExecGraphNodeEquality(graphA.rootNode, graphB.rootNode, isMpi);
}
}
