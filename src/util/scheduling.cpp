#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/scheduling.h>
#include <faabric/util/string_tools.h>

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

void SchedulingDecision::debugPrint()
{
    SPDLOG_DEBUG("-------------- Decision for App: {} ----------------", appId);
    // The app ID will always be set, whereas the group id may not. If the
    // group ID is 0, we want to only add one tab so that columns are formatted
    // correctly upon print
    if (groupId != 0) {
        SPDLOG_DEBUG("MsgId\tAppId\t\tGroupId\t\tGrIdx\tHostIp\t\tHostCap");
    } else {
        SPDLOG_DEBUG("MsgId\tAppId\tGroupId\tGrIdx\tHostIp\t\tHostCap");
    }
    for (int i = 0; i < hosts.size(); i++) {
        SPDLOG_DEBUG("{}\t{}\t{}\t{}\t{}\t{}",
                     messageIds.at(i),
                     appId,
                     groupId,
                     groupIdxs.at(i),
                     hosts.at(i),
                     plannerHosts.at(i)->slots() -
                       plannerHosts.at(i)->usedslots());
    }
    SPDLOG_DEBUG("------------- End Decision for App {} ---------------",
                 appId);
}

bool SchedulingDecision::isSingleHost()
{
    // Always return false if single-host optimisations are switched off
    SystemConfig& conf = getSystemConfig();
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

void SchedulingDecision::removeMessage(const faabric::Message& msg)
{
    nFunctions--;

    // Work out the index for the to-be-deleted message
    auto idxItr = std::find(messageIds.begin(), messageIds.end(), msg.id());
    if (idxItr == messageIds.end()) {
        SPDLOG_ERROR("Attempting to remove a message id ({}) that is not in "
                     "the scheduling decision!",
                     msg.id());
        throw std::runtime_error("Removing non-existant message!");
    }
    int idx = std::distance(messageIds.begin(), idxItr);

    hosts.erase(hosts.begin() + idx);
    messageIds.erase(messageIds.begin() + idx);
    appIdxs.erase(appIdxs.begin() + idx);
    groupIdxs.erase(groupIdxs.begin() + idx);

    // TODO: still done separately
    plannerHosts.at(idx)->set_usedslots(plannerHosts.at(idx)->usedslots() - 1);
    plannerHosts.erase(plannerHosts.begin() + idx);
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
