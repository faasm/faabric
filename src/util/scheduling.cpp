#include "faabric/util/func.h"
#include "faabric/util/string_tools.h"
#include <faabric/util/config.h>
#include <faabric/util/scheduling.h>

namespace faabric::util {

CachedDecision::CachedDecision(const std::vector<std::string>& hostsIn,
                               int groupIdIn)
  : hosts(hostsIn)
  , groupId(groupIdIn)
{}

std::shared_ptr<CachedDecision> DecisionCache::getCachedDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    std::string cacheKey = getCacheKey(req);
    bool hasDecision = cachedDecisions.find(cacheKey) != cachedDecisions.end();

    if (hasDecision) {
        std::shared_ptr<CachedDecision> res = cachedDecisions[cacheKey];

        // Sanity check we've got something the right size
        if (res->getHosts().size() != req->messages().size()) {
            SPDLOG_ERROR("Cached decision for {} has {} hosts, expected {}",
                         faabric::util::funcToString(req),
                         res->getHosts().size(),
                         req->messages().size());

            throw std::runtime_error("Invalid cached scheduling decision");
        }

        return res;
    }

    return nullptr;
}

void DecisionCache::addCachedDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::util::SchedulingDecision& decision)
{
    std::string cacheKey = getCacheKey(req);

    if (req->messages_size() != decision.hosts.size()) {
        SPDLOG_ERROR("Trying to cache decision for {} with wrong size {} != {}",
                     funcToString(req),
                     req->messages_size(),
                     decision.hosts.size());
        throw std::runtime_error("Invalid decision caching");
    }

    SPDLOG_DEBUG("Caching decision for {} x {} app {}, group {}, hosts {}",
                 req->messages().size(),
                 faabric::util::funcToString(req),
                 decision.appId,
                 decision.groupId,
                 faabric::util::vectorToString<std::string>(decision.hosts));

    cachedDecisions[cacheKey] =
      std::make_shared<CachedDecision>(decision.hosts, decision.groupId);
}

std::string DecisionCache::getCacheKey(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return std::to_string(req->messages().at(0).appid()) + "_" +
           std::to_string(req->messages_size());
}

void DecisionCache::clear()
{
    cachedDecisions.clear();
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

bool SchedulingDecision::isSingleHost()
{
    // Work out if this decision is all on this host. If the decision is
    // completely on *another* host, we still count it as not being on a single
    // host, as this host will be the master
    std::string thisHost = faabric::util::getSystemConfig().endpointHost;
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
}
