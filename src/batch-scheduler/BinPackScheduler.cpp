#include <faabric/batch-scheduler/BinPackScheduler.h>
#include <faabric/util/batch.h>
#include <faabric/util/logging.h>
#include <faabric/util/scheduling.h>

namespace faabric::batch_scheduler {
std::shared_ptr<faabric::util::SchedulingDecision>
BinPackScheduler::makeSchedulingDecision(
  const HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<BatchExecuteRequest> req)
{
    // TODO: think about the group id!
    auto decision =
      std::make_shared<faabric::util::SchedulingDecision>(req->appid(), 0);

    SPDLOG_INFO("Hello world!");

    return decision;
}
}
