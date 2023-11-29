#pragma once

#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/proto/faabric.pb.h>

namespace faabric::util {
faabric::PointToPointMappings ptpMappingsFromSchedulingDecision(
  std::shared_ptr<faabric::batch_scheduler::SchedulingDecision> decision);
}
