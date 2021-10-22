#include <faabric/util/scheduling.h>

namespace faabric::util {
SchedulingDecision::SchedulingDecision(uint32_t appIdIn)
  : appId(appIdIn)
{}

void SchedulingDecision::addMessage(const std::string& host,
                                    const faabric::Message& msg)
{
    addMessage(host, msg.id(), msg.appindex());
}

void SchedulingDecision::addMessage(const std::string& host,
                                    int32_t messageId,
                                    int32_t appIdx)
{
    nFunctions++;

    hosts.emplace_back(host);
    messageIds.emplace_back(messageId);
    appIdxs.emplace_back(appIdx);
}

SchedulingDecision SchedulingDecision::fromPointToPointMappings(
  faabric::PointToPointMappings& mappings)
{
    SchedulingDecision decision(mappings.appid());

    for (const auto& m : mappings.mappings()) {
        decision.addMessage(m.host(), m.messageid(), m.recvidx());
    }

    return decision;
}
}
