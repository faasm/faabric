#pragma once

#include "fixtures.h"

#include <faabric/scheduler/ExecGraph.h>
#include <faabric/state/State.h>
#include <faabric/state/StateServer.h>
#include <faabric/util/func.h>
#include <faabric/util/testing.h>

using namespace faabric;

#define SHORT_TEST_TIMEOUT_MS 1000

namespace tests {
void cleanFaabric();

void checkMessageEquality(const faabric::Message& msgA,
                          const faabric::Message& msgB);

void checkExecGraphNodeEquality(const scheduler::ExecGraphNode& nodeA,
                                const scheduler::ExecGraphNode& nodeB);

void checkExecGraphEquality(const scheduler::ExecGraph& graphA,
                            const scheduler::ExecGraph& graphB);

}
