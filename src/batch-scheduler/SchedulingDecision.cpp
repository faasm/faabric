#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

namespace faabric::batch_scheduler {

SchedulingDecision::SchedulingDecision(uint32_t appIdIn, int32_t groupIdIn)
  : appId(appIdIn)
  , groupId(groupIdIn)
{}

bool SchedulingDecision::isSingleHost() const
{
    auto& conf = faabric::util::getSystemConfig();

    std::string thisHost = conf.endpointHost;
    std::set<std::string> hostSet(hosts.begin(), hosts.end());
    return hostSet.size() == 1;
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
    mpiPorts.push_back(0);
}

SchedulingDecision SchedulingDecision::fromPointToPointMappings(
  faabric::PointToPointMappings& mappings)
{
    SchedulingDecision decision(mappings.appid(), mappings.groupid());

    for (const auto& map : mappings.mappings()) {
        decision.addMessage(
          map.host(), map.messageid(), map.appidx(), map.groupidx());
        decision.mpiPorts.at(decision.nFunctions - 1) = map.mpiport();
    }

    return decision;
}

int32_t SchedulingDecision::removeMessage(int32_t messageId)
{
    // Work out the index for the to-be-deleted message
    auto idxItr = std::find(messageIds.begin(), messageIds.end(), messageId);
    if (idxItr == messageIds.end()) {
        SPDLOG_ERROR("Attempting to remove a message id ({}) that is not in "
                     "the scheduling decision!",
                     messageId);
        throw std::runtime_error("Removing non-existant message!");
    }
    int idx = std::distance(messageIds.begin(), idxItr);

    nFunctions--;
    hosts.erase(hosts.begin() + idx);
    messageIds.erase(messageIds.begin() + idx);
    appIdxs.erase(appIdxs.begin() + idx);
    groupIdxs.erase(groupIdxs.begin() + idx);

    int vacatedMpiPort = *(mpiPorts.begin() + idx);
    mpiPorts.erase(mpiPorts.begin() + idx);

    return vacatedMpiPort;
}

std::set<std::string> SchedulingDecision::uniqueHosts()
{
    return std::set<std::string>(hosts.begin(), hosts.end());
}

void SchedulingDecision::print(const std::string& logLevel)
{
    std::string printedText = "Printing scheduling decision:";
    printedText += fmt::format(
      "\n-------------- Decision for App: {} ----------------\n", appId);
    printedText += "MsgId\tAppId\tGroupId\tGrIdx\tHostIp\t\tPort\n";
    // Modulo a big number so that we can get the UUIDs to fit within one tab
    int formatBase = 1e6;
    for (int i = 0; i < hosts.size(); i++) {
        printedText += fmt::format("{}\t{}\t{}\t{}\t{}\t{}\n",
                                   messageIds.at(i) % formatBase,
                                   appId % formatBase,
                                   groupId % formatBase,
                                   groupIdxs.at(i),
                                   hosts.at(i),
                                   mpiPorts.at(i));
    }
    printedText += fmt::format(
      "------------- End Decision for App {} ---------------", appId);

    if (logLevel == "debug") {
        SPDLOG_DEBUG(printedText);
    } else if (logLevel == "info") {
        SPDLOG_INFO(printedText);
    } else if (logLevel == "warn") {
        SPDLOG_WARN(printedText);
    } else if (logLevel == "error") {
        SPDLOG_ERROR(printedText);
    } else {
        SPDLOG_ERROR("Unrecognised log level: {}", logLevel);
    }
}
}
