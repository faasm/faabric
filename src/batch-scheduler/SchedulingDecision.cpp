#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

namespace faabric::batch_scheduler {

SchedulingDecision::SchedulingDecision(uint32_t appIdIn, int32_t groupIdIn)
  : appId(appIdIn)
  , groupId(groupIdIn)
{}

bool SchedulingDecision::isSingleHost()
{
    // Always return false if single-host optimisations are switched off
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    if (conf.noSingleHostOptimisations == 1) {
        return false;
    }

    std::string thisHost = conf.endpointHost;
    return std::all_of(hosts.begin(), hosts.end(), [&](const std::string& s) {
        return s == thisHost;
    });
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
}

SchedulingDecision SchedulingDecision::fromPointToPointMappings(
  faabric::PointToPointMappings& mappings)
{
    SchedulingDecision decision(mappings.appid(), mappings.groupid());

    for (const auto& m : mappings.mappings()) {
        decision.addMessage(m.host(), m.messageid(), m.appidx(), m.groupidx());
    }

    return decision;
}

void SchedulingDecision::removeMessage(int32_t messageId)
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
    printedText += "MsgId\tAppId\tGroupId\tGrIdx\tHostIp\n";
    // Modulo a big number so that we can get the UUIDs to fit within one tab
    int formatBase = 1e6;
    for (int i = 0; i < hosts.size(); i++) {
        printedText += fmt::format("{}\t{}\t{}\t{}\t{}\n",
                                   messageIds.at(i) % formatBase,
                                   appId % formatBase,
                                   groupId % formatBase,
                                   groupIdxs.at(i),
                                   hosts.at(i));
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
