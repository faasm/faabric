#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

namespace faabric::batch_scheduler {

SchedulingDecision::SchedulingDecision(uint32_t appIdIn, int32_t groupIdIn)
  : appId(appIdIn)
  , groupId(groupIdIn)
{}

bool SchedulingDecision::isSingleHost()
{
    // Always return false if single-host optimisations are switched off
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    if (conf.noSingleHostOptimisations == 1) {
        return false;
    }

    std::string thisHost = conf.endpointHost;
    return std::all_of(hosts.begin(), hosts.end(), [&](const std::string& s) {
        return s == thisHost;
    });
}

void SchedulingDecision::addMessage(const std::string& host,
                                    const faabric::Message& msg)
{
    addMessage(host, msg.id(), msg.appidx(), msg.groupidx());
}

void SchedulingDecision::addMessage(const std::string& host,
                                    int32_t messageId,
                                    int32_t appIdx,
                                    int32_t groupIdx)
{
    nFunctions++;

    hosts.emplace_back(host);
    messageIds.emplace_back(messageId);
    appIdxs.emplace_back(appIdx);
    groupIdxs.emplace_back(groupIdx);
}

SchedulingDecision SchedulingDecision::fromPointToPointMappings(
  faabric::PointToPointMappings& mappings)
{
    SchedulingDecision decision(mappings.appid(), mappings.groupid());

    for (const auto& m : mappings.mappings()) {
        decision.addMessage(m.host(), m.messageid(), m.appidx(), m.groupidx());
    }

    return decision;
}

std::set<std::string> SchedulingDecision::uniqueHosts()
{
    return std::set<std::string>(hosts.begin(), hosts.end());
}

void SchedulingDecision::print()
{
    SPDLOG_DEBUG("-------------- Decision for App: {} ----------------", appId);
    SPDLOG_DEBUG("MsgId\tAppId\tGroupId\tGrIdx\tHostIp");
    // Modulo a big number so that we can get the UUIDs to fit within one tab
    int formatBase = 1e6;
    for (int i = 0; i < hosts.size(); i++) {
        SPDLOG_DEBUG("{}\t{}\t{}\t{}\t{}",
                     messageIds.at(i) % formatBase,
                     appId % formatBase,
                     groupId % formatBase,
                     groupIdxs.at(i),
                     hosts.at(i));
    }
    SPDLOG_DEBUG("------------- End Decision for App {} ---------------",
                 appId);
}
}
