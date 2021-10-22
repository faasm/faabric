#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <faabric/proto/faabric.pb.h>

namespace faabric::util {

class SchedulingDecision
{
  public:
    SchedulingDecision(uint32_t appIdIn, int32_t nFunctionsIn);
    uint32_t appId = 0;

    int32_t nFunctions = 0;

    std::vector<uint32_t> messageIds;
    std::vector<std::string> hosts;

    std::vector<uint32_t> appIdxs;

    std::string returnHost;

    void addMessage(const std::string &host, const faabric::Message &msg);
};

}
