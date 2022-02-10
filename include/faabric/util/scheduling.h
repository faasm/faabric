#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/locks.h>

namespace faabric::util {

class CachedDecision
{
  public:
    std::vector<std::string> getHosts();

  private:
    std::vector<std::string> hosts;
};

class DecisionCache
{
  public:
    CachedDecision getCachedDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    bool hasCachedDecision(std::shared_ptr<faabric::BatchExecuteRequest> req);

    std::string getCacheKey(std::shared_ptr<faabric::BatchExecuteRequest> req);

  private:
    std::shared_mutex mx;

    std::unordered_map<std::string, int> cachedGroupIds;
    std::unordered_map<std::string, std::vector<std::string>>
      cachedDecisionHosts;
};

DecisionCache& getSchedulingDecisionCache();

class SchedulingDecision
{
  public:
    static SchedulingDecision fromPointToPointMappings(
      faabric::PointToPointMappings& mappings);

    SchedulingDecision(uint32_t appIdIn, int32_t groupIdIn);

    uint32_t appId = 0;

    int32_t groupId = 0;

    int32_t nFunctions = 0;

    std::vector<std::string> hosts;

    std::vector<int32_t> messageIds;

    std::vector<int32_t> appIdxs;

    std::vector<int32_t> groupIdxs;

    std::string returnHost;

    void addMessage(const std::string& host, const faabric::Message& msg);

    void addMessage(const std::string& host, int32_t messageId, int32_t appIdx);

    void addMessage(const std::string& host,
                    int32_t messageId,
                    int32_t appIdx,
                    int32_t groupIdx);
};

// Scheduling topology hints help the scheduler decide which host to assign new
// requests in a batch.
//  - NORMAL: bin-packs requests to slots in hosts starting from the master
//            host, and overloadds the master if it runs out of resources.
//  - FORCE_LOCAL: force local execution irrespective of the available
//                 resources.
//  - NEVER_ALONE: never allocates a single (non-master) request to a host
//                 without other requests of the batch.
//  - UNDERFULL: schedule up to 50% of the master hosts' capacity to force
//               migration opportunities to appear.
enum SchedulingTopologyHint
{
    NORMAL,
    FORCE_LOCAL,
    NEVER_ALONE,
    UNDERFULL,
};

// Map to convert input strings to scheduling topology hints and the other way
// around
const std::unordered_map<std::string, SchedulingTopologyHint>
  strToTopologyHint = {
      { "NORMAL", SchedulingTopologyHint::NORMAL },
      { "FORCE_LOCAL", SchedulingTopologyHint::FORCE_LOCAL },
      { "NEVER_ALONE", SchedulingTopologyHint::NEVER_ALONE },
      { "UNDERFULL", SchedulingTopologyHint::UNDERFULL },
  };

const std::unordered_map<SchedulingTopologyHint, std::string>
  topologyHintToStr = {
      { SchedulingTopologyHint::NORMAL, "NORMAL" },
      { SchedulingTopologyHint::FORCE_LOCAL, "FORCE_LOCAL" },
      { SchedulingTopologyHint::NEVER_ALONE, "NEVER_ALONE" },
      { SchedulingTopologyHint::UNDERFULL, "UNDERFULL" },
  };

// Migration strategies help the scheduler decide wether the scheduling decision
// for a batch request could be changed with the new set of available resources.
// - BIN_PACK: sort hosts by the number of functions from the batch they are
//             running. Bin-pack batches in increasing order to hosts in
//             decreasing order.
// - EMPTY_HOSTS: pack batches in increasing order to empty hosts.
enum MigrationStrategy
{
    BIN_PACK,
    EMPTY_HOSTS
};
}
