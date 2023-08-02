#include <faabric/batch-scheduler/DecisionCache.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/string_tools.h>

namespace faabric::batch_scheduler {
CachedDecision::CachedDecision(const std::vector<std::string>& hostsIn,
                               int groupIdIn)
  : hosts(hostsIn)
  , groupId(groupIdIn)
{}

std::shared_ptr<CachedDecision> DecisionCache::getCachedDecision(
  std::shared_ptr<BatchExecuteRequest> req)
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

void DecisionCache::addCachedDecision(std::shared_ptr<BatchExecuteRequest> req,
                                      SchedulingDecision& decision)
{
    std::string cacheKey = getCacheKey(req);

    if (req->messages_size() != decision.hosts.size()) {
        SPDLOG_ERROR("Trying to cache decision for {} with wrong size {} != {}",
                     faabric::util::funcToString(req),
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
}
