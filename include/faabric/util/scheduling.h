#include <cstdint>
#include <string>
#include <vector>

namespace faabric::util {

class SchedulingDecision
{
  public:
    uint32_t appId;
    std::vector<uint32_t> appIdxs;
    std::vector<uint32_t> messageIds;
    std::vector<std::string> hosts;
};

}
