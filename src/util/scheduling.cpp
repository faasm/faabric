#include <faabric/util/scheduling.h>

namespace faabric::util {
SchedulingDecision::SchedulingDecision(uint32_t appIdIn)
  : appId(appIdIn)
{}

void SchedulingDecision::addMessage(const std::string& host,
                                    const faabric::Message& msg)
{
    nFunctions++;

    messageIds.emplace_back(msg.id());
    hosts.emplace_back(host);
    appIdxs.emplace_back(msg.appindex());
}
}
