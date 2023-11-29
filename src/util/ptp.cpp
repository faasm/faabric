#include <faabric/util/ptp.h>

namespace faabric::util {
faabric::PointToPointMappings ptpMappingsFromSchedulingDecision(
  std::shared_ptr<faabric::batch_scheduler::SchedulingDecision> decision)
{
    faabric::PointToPointMappings mappings;
    mappings.set_appid(decision->appId);
    mappings.set_groupid(decision->groupId);
    for (int i = 0; i < decision->hosts.size(); i++) {
        auto* mapping = mappings.add_mappings();
        mapping->set_host(decision->hosts.at(i));
        mapping->set_messageid(decision->messageIds.at(i));
        mapping->set_appidx(decision->appIdxs.at(i));
        mapping->set_groupidx(decision->groupIdxs.at(i));
    }

    return mappings;
}
}
