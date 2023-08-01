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

    auto isFirstHostLarger = [&](const Host& hostA, const Host& hostB) -> bool {
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

    // Helper lambda to get the frequency of messags at each host (useful to
    // sort hosts maximising locality)
    auto getHostFreqCount = [inFlightReqs,
                             req]() -> std::map<std::string, int> {
        std::map<std::string, int> hostFreqCount;

        auto oldDecision = inFlightReqs.at(req->appid()).second;
        for (auto host : oldDecision->hosts) {
            hostFreqCount[host] += 1;
        }

        return hostFreqCount;
    };

    switch (decisionType) {
        case DecisionType::NEW: {
            // For a NEW decision type, the BinPack scheduler just sorts the
            // hosts in decreasing order of capacity, and bin-packs messages
            // to hosts in this order
            std::sort(
              sortedHosts.begin(), sortedHosts.end(), isFirstHostLarger);
            break;
        }
        case DecisionType::SCALE_CHANGE: {
            // If we are changing the scale of a running app (i.e. via chaining
            // or thread/process forking) we want to prioritise co-locating
            // as much as possible. This means that we will sort first by the
            // frequency of messages of the running app, and second with the
            // same criteria than NEW
            // IMPORTANT: a SCALE_CHANGE request with 4 messages means that we
            // want to add 4 NEW messages to the running app (not that the new
            // total count is 4)
            auto hostFreqCount = getHostFreqCount();
            std::sort(sortedHosts.begin(),
                      sortedHosts.end(),
                      [&](auto hostA, auto hostB) -> bool {
                          int numInHostA = hostFreqCount.contains(getIp(hostA))
                                             ? hostFreqCount.at(getIp(hostA))
                                             : 0;
                          int numInHostB = hostFreqCount.contains(getIp(hostB))
                                             ? hostFreqCount.at(getIp(hostB))
                                             : 0;

                          // If at least one of the hosts has messages for this
                          // request, return the host with the more messages for
                          // this request (note that it is possible that this
                          // host has no available slots at all, in this case we
                          // will just pack 0 messages here but we still want to
                          // sort it first nontheless)
                          if (numInHostA != numInHostB) {
                              return numInHostA > numInHostB;
                          }

                          // In case of a tie, use the same criteria than NEW
                          return isFirstHostLarger(hostA, hostB);
                      });
            break;
        }
        default: {
            SPDLOG_ERROR("Unrecognised decision type: {}", decisionType);
            throw std::runtime_error("Unrecognised decision type");
        }
    }

    return sortedHosts;
}

// The BinPack's scheduler decision algorithm is very simple. It first sorts
// hosts (i.e. bins) in a specific order (depending on the scheduling type),
// and then starts filling bins from begining to end, until it runs out of
// messages to schedule
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

    // Assign slots from the list (i.e. bin-pack)
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
