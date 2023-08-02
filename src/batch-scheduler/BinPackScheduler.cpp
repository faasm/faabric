#include <faabric/batch-scheduler/BinPackScheduler.h>
#include <faabric/util/batch.h>
#include <faabric/util/logging.h>
#include <faabric/util/scheduling.h>

namespace faabric::batch_scheduler {

static std::map<std::string, int> getHostFreqCount(
  std::shared_ptr<faabric::util::SchedulingDecision> decision)
{
    std::map<std::string, int> hostFreqCount;
    for (auto host : decision->hosts) {
        hostFreqCount[host] += 1;
    }

    return hostFreqCount;
}

// Given a new decision that improves on an old decision (i.e. to migrate), we
// want to make sure that we minimise the number of migration requests we send.
// This is, we want to keep as many host-message scheduling in the old decision
// as possible, and also have the overall locality of the new decision (i.e.
// the host-message histogram)
static std::shared_ptr<faabric::util::SchedulingDecision>
minimiseNumOfMigrations(
  std::shared_ptr<faabric::util::SchedulingDecision> newDecision,
  std::shared_ptr<faabric::util::SchedulingDecision> oldDecision)
{
    auto decision = std::make_shared<faabric::util::SchedulingDecision>(
      oldDecision->appId, oldDecision->groupId);

    // We want to maintain the new decision's host-message histogram
    auto hostFreqCount = getHostFreqCount(newDecision);

    // Helper function to find the next host in the histogram with slots
    auto nextHostWithSlots = [&hostFreqCount]() -> std::string {
        for (auto [ip, slots] : hostFreqCount) {
            if (slots > 0) {
                return ip;
            }
        }

        // Unreachable (in this context)
        throw std::runtime_error("No next host with slots found!");
    };

    assert(newDecision->hosts.size() == oldDecision->hosts.size());
    for (int i = 0; i < newDecision->hosts.size(); i++) {
        // If both decisions schedule this message to the same host great, as
        // we can keep the old scheduling
        if (newDecision->hosts.at(i) == oldDecision->hosts.at(i) &&
            hostFreqCount.at(newDecision->hosts.at(i)) > 0) {
            decision->addMessage(oldDecision->hosts.at(i),
                                 oldDecision->messageIds.at(i),
                                 oldDecision->appIdxs.at(i),
                                 oldDecision->groupIdxs.at(i));
            hostFreqCount.at(oldDecision->hosts.at(i)) -= 1;
            continue;
        }

        // If not, assign the old decision as long as we still can (i.e. as
        // long as we still have slots in the histogram (note that it could be
        // that the old host is not in the new histogram at all)
        if (hostFreqCount.contains(oldDecision->hosts.at(i)) &&
            hostFreqCount.at(oldDecision->hosts.at(i)) > 0) {
            decision->addMessage(oldDecision->hosts.at(i),
                                 oldDecision->messageIds.at(i),
                                 oldDecision->appIdxs.at(i),
                                 oldDecision->groupIdxs.at(i));
            hostFreqCount.at(oldDecision->hosts.at(i)) -= 1;
            continue;
        }

        // If we can't assign the host from the old decision, then it means
        // that that message MUST be migrated, so it doesn't really matter
        // which of the hosts from the new migration we pick (as the new
        // decision is optimal in terms of bin-packing), as long as there are
        // still slots in the histogram
        auto nextHost = nextHostWithSlots();
        decision->addMessage(nextHost,
                             oldDecision->messageIds.at(i),
                             oldDecision->appIdxs.at(i),
                             oldDecision->groupIdxs.at(i));
        hostFreqCount.at(nextHost) -= 1;
    }

    // Assert that we have preserved the new decision's host-message histogram
    // (use the pre-processor macro as we assert repeatedly in the loop, so we
    // want to avoid having an empty loop in non-debug mode)
#ifndef NDEBUG
    for (auto [host, freq] : hostFreqCount) {
        assert(freq == 0);
    }
#endif

    return decision;
}

// For the BinPack scheduler, a decision is better than another one if it spans
// less hosts. In case of a tie, we calculate the number of cross-VM links
// (i.e. better locality, or better packing)
bool BinPackScheduler::isFirstDecisionBetter(
  std::shared_ptr<faabric::util::SchedulingDecision> decisionA,
  std::shared_ptr<faabric::util::SchedulingDecision> decisionB)
{
    auto getLocalityScore =
      [](std::shared_ptr<faabric::util::SchedulingDecision> decision)
      -> std::pair<int, int> {
        // First, calculate the host-message histogram (or frequency count)
        std::map<std::string, int> hostFreqCount;
        for (auto host : decision->hosts) {
            hostFreqCount[host] += 1;
        }

        // If scheduling is single host, return one host and 0 cross-host links
        if (hostFreqCount.size() == 1) {
            return std::make_pair(1, 0);
        }

        // Else, do the product of all entries
        int score = 1;
        for (auto [host, freq] : hostFreqCount) {
            score = score * freq;
        }

        return std::make_pair(hostFreqCount.size(), score);
    };

    auto scoreA = getLocalityScore(decisionA);
    auto scoreB = getLocalityScore(decisionB);

    // The first decision is better if it has a LOWER host set size
    if (scoreA.first != scoreB.first) {
        return scoreA.first < scoreB.first;
    }

    // The first decision is better if it has a LOWER locality score
    return scoreA.second < scoreB.second;
}

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
    auto getHostFreqCount =
      [](std::shared_ptr<faabric::util::SchedulingDecision> decision)
      -> std::map<std::string, int> {
        std::map<std::string, int> hostFreqCount;

        for (auto host : decision->hosts) {
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
            auto oldDecision = inFlightReqs.at(req->appid()).second;
            auto hostFreqCount = getHostFreqCount(oldDecision);
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
        case DecisionType::DIST_CHANGE: {
            // When migrating, we want to know if the provided for app (which
            // is already in-flight) can be improved according to the bin-pack
            // scheduling logic. This is equivalent to saying that the number
            // of cross-vm links can be reduced (i.e. we improve locality)
            auto oldDecision = inFlightReqs.at(req->appid()).second;
            auto hostFreqCount = getHostFreqCount(oldDecision);
            std::sort(sortedHosts.begin(),
                      sortedHosts.end(),
                      // TODO: this is shared with SCALE_CHANGE, so abstract
                      // away FIXME
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
            // Before returning the sorted hosts for dist change, we subtract
            // all slots occupied by the application we want to migrate (note
            // that we want to take into account for the sorting)
            for (auto h : sortedHosts) {
                if (hostFreqCount.contains(getIp(h))) {
                    freeSlots(h, hostFreqCount.at(getIp(h)));
                }
            }
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
            break;
        }

        // Otherwise, it means that we have exhausted this host, and need to
        // check in the next one
        it++;
    }

    // If we still have enough slots to schedule, we are out of slots
    if (numLeftToSchedule > 0) {
        return std::make_shared<faabric::util::SchedulingDecision>(
          NOT_ENOUGH_SLOTS_DECISION);
    }

    // In case of a DIST_CHANGE decision (i.e. migration), we want to make sure
    // that the new decision is better than the previous one
    if (decisionType == DecisionType::DIST_CHANGE) {
        auto oldDecision = inFlightReqs.at(req->appid()).second;
        if (isFirstDecisionBetter(decision, oldDecision)) {
            // If we are sending a better migration, make sure that we minimise
            // the number of migrations to be done
            return minimiseNumOfMigrations(decision, oldDecision);
        }

        return std::make_shared<faabric::util::SchedulingDecision>(
          DO_NOT_MIGRATE_DECISION);
    }

    return decision;
}
}
