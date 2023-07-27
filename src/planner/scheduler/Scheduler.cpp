#include <faabric/planner/scheduler/Scheduler.h>
#include <faabric/uitl/batch.h>
#include <faabric/util/logging.h>
#include <faabric/util/scheduling.h>

namespace faabric::planner::scheduler {
std::shared_ptr<faabric::util::SchedulingDecision>
BinPackScheduler::makeSchedulingDecision(
    std::shared_ptr<faabric::BatchExecuteRequest> req,
    faabric::util::SchedulingTopologyHint topologyHint)
{
    SPDLOG_INFO("Hello world!");
}
}
