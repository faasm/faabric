#pragma once

#include <faabric/proto/faabric.pb.h>

namespace faabric::batch_scheduler {
// Scheduling topology hints help the scheduler decide which host to assign new
// requests in a batch.
//  - NONE: bin-packs requests to slots in hosts starting from the main
//          host, and overloadds the main if it runs out of resources.
//  - FORCE_LOCAL: force local execution irrespective of the available
//                 resources.
//  - NEVER_ALONE: never allocates a single (non-main) request to a host
//                 without other requests of the batch.
//  - UNDERFULL: schedule up to 50% of the main hosts' capacity to force
//               migration opportunities to appear.
enum SchedulingTopologyHint
{
    NONE,
    CACHED,
    FORCE_LOCAL,
    NEVER_ALONE,
    UNDERFULL,
};

// Map to convert input strings to scheduling topology hints and the other way
// around
const std::unordered_map<std::string, SchedulingTopologyHint>
  strToTopologyHint = {
      { "NONE", SchedulingTopologyHint::NONE },
      { "CACHED", SchedulingTopologyHint::CACHED },
      { "FORCE_LOCAL", SchedulingTopologyHint::FORCE_LOCAL },
      { "NEVER_ALONE", SchedulingTopologyHint::NEVER_ALONE },
      { "UNDERFULL", SchedulingTopologyHint::UNDERFULL },
  };

const std::unordered_map<SchedulingTopologyHint, std::string>
  topologyHintToStr = {
      { SchedulingTopologyHint::NONE, "NONE" },
      { SchedulingTopologyHint::CACHED, "CACHED" },
      { SchedulingTopologyHint::FORCE_LOCAL, "FORCE_LOCAL" },
      { SchedulingTopologyHint::NEVER_ALONE, "NEVER_ALONE" },
      { SchedulingTopologyHint::UNDERFULL, "UNDERFULL" },
  };

// TODO(planner-schedule): remove these strategies
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

class SchedulingDecision
{
  public:
    static SchedulingDecision fromPointToPointMappings(
      faabric::PointToPointMappings& mappings);

    SchedulingDecision(uint32_t appIdIn, int32_t groupIdIn);

    bool operator==(const SchedulingDecision& rhs) const = default;

    uint32_t appId = 0;

    int32_t groupId = 0;

    int32_t nFunctions = 0;

    std::vector<std::string> hosts;

    std::vector<int32_t> messageIds;

    std::vector<int32_t> appIdxs;

    std::vector<int32_t> groupIdxs;

    std::vector<int32_t> mpiPorts;

    std::string returnHost;

    /**
     * Work out if this decision is all in one host. If the decision is
     * completely on *another* host, we still count it as not being on a single
     * host, as this host will be the main.
     *
     * Will always return false if single host optimisations are switched off.
     */
    bool isSingleHost() const;

    void addMessage(const std::string& host, const faabric::Message& msg);

    void addMessage(const std::string& host, int32_t messageId, int32_t appIdx);

    void addMessage(const std::string& host,
                    int32_t messageId,
                    int32_t appIdx,
                    int32_t groupIdx);

    // Returns the MPI port that we have vacated
    int32_t removeMessage(int32_t messageId);

    std::set<std::string> uniqueHosts();

    void print(const std::string& logLevel = "debug");
};

}
