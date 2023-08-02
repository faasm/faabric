#pragma once

#include <faabric/batch-scheduler/SchedulingDecision.h>

#include <string>
#include <vector>

namespace faabric::batch_scheduler {
/**
 * A record of a decision already taken for the given size of batch request
 * for the given function. This doesn't contain the messages themselves,
 * just the hosts and group ID that was used.
 */
class CachedDecision
{
  public:
    CachedDecision(const std::vector<std::string>& hostsIn, int groupIdIn);

    std::vector<std::string> getHosts() { return hosts; }

    int getGroupId() const { return groupId; }

  private:
    std::vector<std::string> hosts;
    int groupId = 0;
};

/**
 * Repository for cached scheduling decisions. Object is not thread safe as we
 * assume only a single executor will be caching decisions for a given function
 * and size of batch request on one host at a time.
 */
class DecisionCache
{
  public:
    std::shared_ptr<CachedDecision> getCachedDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    void addCachedDecision(std::shared_ptr<BatchExecuteRequest> req,
                           SchedulingDecision& decision);

    void clear();

  private:
    std::string getCacheKey(std::shared_ptr<BatchExecuteRequest> req);

    std::unordered_map<std::string, std::shared_ptr<CachedDecision>>
      cachedDecisions;
};

DecisionCache& getSchedulingDecisionCache();
}
