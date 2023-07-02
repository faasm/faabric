#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/util/logging.h>

using namespace faabric::util;

namespace tests {
void checkExecGraphNodeEquality(const ExecGraphNode& nodeA,
                                const ExecGraphNode& nodeB)
{
    // Check the message itself
    checkMessageEquality(nodeA.msg, nodeB.msg);

    if (nodeA.children.size() != nodeB.children.size()) {
        FAIL(fmt::format("Children not same size: {} vs {}",
                         nodeA.children.size(),
                         nodeB.children.size()));
    }

    // Assume children are in same order
    for (int i = 0; i < nodeA.children.size(); i++) {
        checkExecGraphNodeEquality(nodeA.children.at(i), nodeB.children.at(i));
    }
}

void checkExecGraphEquality(const ExecGraph& graphA, const ExecGraph& graphB)
{
    checkExecGraphNodeEquality(graphA.rootNode, graphB.rootNode);
}
}
