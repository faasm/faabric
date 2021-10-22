#include <faabric/util/scheduling.h>

namespace faabric::util {
SchedulingDecision::SchedulingDecision(uint32_t appIdIn, int32_t nFunctionsIn)
  : appId(appIdIn)
  , nFunctions(nFunctionsIn)
  , messageIds(nFunctions, 0)
  , hosts(nFunctions, "")
  , appIdxs(nFunctions, 0)
{}

void SchedulingDecision::addMessage(const std::string &host, const faabric::Message &msg) {

}
}
