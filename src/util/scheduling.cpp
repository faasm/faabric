#include <faabric/util/scheduling.h>

namespace faabric::util {

CachedDecision DecisionCache::getCachedDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{}

bool DecisionCache::hasCachedDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    std::string cacheKey = getCacheKey(req);
    bool res = false;
    {
        faabric::util::SharedLock lock(mx);
        res = cachedDecisionHosts.find(cacheKey) != cachedDecisionHosts.end();
    }

    return res;
}

std::string DecisionCache::getCacheKey(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return std::to_string(req->messages().at(0).appid()) + "_" +
           std::to_string(req->messages_size());
}

DecisionCache& getSchedulingDecisionCache()
{
    static DecisionCache c;
    return c;
}

SchedulingDecision::SchedulingDecision(uint32_t appIdIn, int32_t groupIdIn)
  : appId(appIdIn)
  , groupId(groupIdIn)
{}

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
}
