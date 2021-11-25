#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <faabric/proto/faabric.pb.h>

namespace faabric::util {

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
//  - NEVER_ALONE: never allocates a single (non-master) request to a host
//                 without other requests of the batch.
enum SchedulingTopologyHint
{
    NORMAL,
    FORCE_LOCAL,
    NEVER_ALONE
};
}
