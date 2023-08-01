#include <faabric/batch-scheduler/BinPackScheduler.h>
#include <faabric/util/batch.h>
#include <faabric/util/logging.h>
#include <faabric/util/scheduling.h>

namespace faabric::batch_scheduler {

std::vector<Host> BinPackScheduler::getSortedHosts(
  const HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  const DecisionType& decisionType)
{
    std::vector<Host> sortedHosts;
    for (auto [ip, host] : hostMap) {
        sortedHosts.push_back(host);
    }

    auto isHostSmaller = [&](const Host& hostA, const Host& hostB) -> bool {
        // The BinPack scheduler sorts hosts by number of available slots
        int nAvailableA = numSlotsAvailable(hostA);
        int nAvailableB = numSlotsAvailable(hostB);
        if (nAvailableA != nAvailableB) {
            return nAvailableA > nAvailableB;
        }

        // In case of a tie, it will pick larger hosts first
        int nSlotsA = numSlots(hostA);
        int nSlotsB = numSlots(hostB);
        if (nSlotsA != nSlotsB) {
            return nSlotsA > nSlotsB;
        }

        // Lastly, in case of a tie, return the largest host alphabetically
        return getIp(hostA) > getIp(hostB);
    };

    switch (decisionType) {
        case DecisionType::NEW: {
            std::sort(sortedHosts.begin(), sortedHosts.end(), isHostSmaller);
            break;
        }
        default: {
            SPDLOG_ERROR("Unrecognised decision type: {}", decisionType);
            throw std::runtime_error("Unrecognised decision type");
        }
    }

    return sortedHosts;
}

std::shared_ptr<faabric::util::SchedulingDecision>
BinPackScheduler::makeSchedulingDecision(
  const HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<BatchExecuteRequest> req)
{
    // TODO: think about the group id!
    auto decision =
      std::make_shared<faabric::util::SchedulingDecision>(req->appid(), 0);

    // Get the sorted list of hosts
    auto decisionType = getDecisionType(inFlightReqs, req);
    auto sortedHosts = getSortedHosts(hostMap, inFlightReqs, req, decisionType);

    // Assign slots from the list
    auto it = sortedHosts.begin();
    int numLeftToSchedule = req->messages_size();
    int msgIdx = 0;
    while (it < sortedHosts.end()) {
        // Calculate how many slots can we assign to this host (assign as many
        // as possible)
        int numOnThisHost =
          std::min<int>(numLeftToSchedule, numSlotsAvailable(*it));
        for (int i = 0; i < numOnThisHost; i++) {
            decision->addMessage(getIp(*it), req->messages(msgIdx));
            msgIdx++;
        }

        // Update the number of messages left to schedule
        numLeftToSchedule -= numOnThisHost;

        // If there are no more messages to schedule, we are done
        if (numLeftToSchedule == 0) {
            return decision;
        }

        // Otherwise, it means that we have exhausted this host, and need to
        // check in the next one
        it++;
    }

    // If we reach this point, it means that we don't have enough slots
    return std::make_shared<faabric::util::SchedulingDecision>(
      NOT_ENOUGH_SLOTS_DECISION);
}
}
