#pragma once

#include <faabric/scheduler/ExecGraph.h>
#include <faabric/state/State.h>
#include <faabric/state/StateServer.h>
#include <faabric/util/func.h>

using namespace faabric;

namespace tests {
void cleanFaabric();

void checkMessageEquality(const faabric::Message& msgA,
                          const faabric::Message& msgB);

void checkExecGraphNodeEquality(const scheduler::ExecGraphNode& nodeA,
                                const scheduler::ExecGraphNode& nodeB);

void checkExecGraphEquality(const scheduler::ExecGraph& graphA,
                            const scheduler::ExecGraph& graphB);

}