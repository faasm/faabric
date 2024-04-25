#include <faabric/batch-scheduler/CompactScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/batch.h>
#include <faabric/util/logging.h>

namespace faabric::batch_scheduler {

static std::map<std::string, int> getHostFreqCount(
  std::shared_ptr<SchedulingDecision> decision)
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
// NOTE: keep in mind that the newDecision has the right host histogram, but
// the messages may be completely out-of-order
static std::shared_ptr<SchedulingDecision> minimiseNumOfMigrations(
  std::shared_ptr<SchedulingDecision> newDecision,
  std::shared_ptr<SchedulingDecision> oldDecision)
{
    auto decision = std::make_shared<SchedulingDecision>(oldDecision->appId,
                                                         oldDecision->groupId);

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

    // First we try to allocate to each message the same host they used to have
    for (int i = 0; i < oldDecision->hosts.size(); i++) {
        auto oldHost = oldDecision->hosts.at(i);

        if (hostFreqCount.contains(oldHost) && hostFreqCount.at(oldHost) > 0) {
            decision->addMessageInPosition(i,
                                           oldHost,
                                           oldDecision->messageIds.at(i),
                                           oldDecision->appIdxs.at(i),
                                           oldDecision->groupIdxs.at(i),
                                           oldDecision->mpiPorts.at(i));

            hostFreqCount.at(oldHost) -= 1;
        }
    }

    // Second we allocate the rest
    for (int i = 0; i < oldDecision->hosts.size(); i++) {
        if (decision->nFunctions <= i || decision->hosts.at(i).empty()) {

            auto nextHost = nextHostWithSlots();
            decision->addMessageInPosition(i,
                                           nextHost,
                                           oldDecision->messageIds.at(i),
                                           oldDecision->appIdxs.at(i),
                                           oldDecision->groupIdxs.at(i),
                                           -1);

            hostFreqCount.at(nextHost) -= 1;
        }
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

bool CompactScheduler::isFirstDecisionBetter(
  std::shared_ptr<SchedulingDecision> decisionA,
  std::shared_ptr<SchedulingDecision> decisionB)
{
    throw std::runtime_error("Method not supported for COMPACT scheduler");
}

HostMap deepCopyHostMap(const HostMap& hostMap)
{
    HostMap newHostMap;

    for (const auto& [ip, host] : hostMap) {
        newHostMap[ip] =
          std::make_shared<HostState>(host->ip, host->slots, host->usedSlots);
    }

    return newHostMap;
}

// For the Compact scheduler, a decision is better than another one if the
// total number of empty hosts has increased
bool CompactScheduler::isFirstDecisionBetter(
  HostMap& hostMap,
  std::shared_ptr<SchedulingDecision> newDecision,
  std::shared_ptr<SchedulingDecision> oldDecision)
{
    auto getNumFreeHosts = [](const HostMap& hostMap) -> int {
        int numFreeHosts = 0;

        for (const auto& [ip, host] : hostMap) {
            if (host->usedSlots == 0) {
                numFreeHosts++;
            }
        }

        return numFreeHosts;
    };

    auto updateHostMapWithDecision =
      [](const HostMap& hostMap,
         std::shared_ptr<SchedulingDecision> decision,
         const std::string& opr) -> HostMap {
        // Be explicit about copying the host maps here
        HostMap newHostMap = deepCopyHostMap(hostMap);

        for (const auto& hostIp : decision->hosts) {
            try {
                if (opr == "add") {
                    newHostMap.at(hostIp)->usedSlots++;
                } else if (opr == "subtract") {
                    newHostMap.at(hostIp)->usedSlots--;
                }
            } catch (std::exception& e) {
                SPDLOG_ERROR("Error accesing host {} in new host map!", hostIp);
                for (const auto& [ip, host] : newHostMap) {
                    SPDLOG_ERROR("{}: {}/{}", ip, host->usedSlots, host->slots);
                }
            }
        }

        return newHostMap;
    };

    // Here we compare the number of free hosts in the original decision and
    // in the new one. Note that, as part of getSortedHosts, we have already
    // subtrated the old decision from the host map, so we need to add it again

    auto originalHostMap =
      updateHostMapWithDecision(hostMap, oldDecision, "add");
    int numFreeHostsBefore = getNumFreeHosts(originalHostMap);

    // Update the host map by removing the old decision and adding the new one

    auto newHostMap = updateHostMapWithDecision(hostMap, newDecision, "add");
    int numFreeHostsAfter = getNumFreeHosts(newHostMap);

    // The first (new) decision is better if it has MORE free hosts
    return numFreeHostsAfter > numFreeHostsBefore;
}

// Filter-out from the host map all nodes that are executing requests from a
// different user
void filterHosts(HostMap& hostMap,
                 const InFlightReqs& inFlightReqs,
                 std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    // We temporarily use the request subtype field to attach a user id for our
    // multi-tenant simulations
    int thisUserId = req->subtype();

    for (const auto& [appId, inFlightPair] : inFlightReqs) {
        if (inFlightPair.first->subtype() == thisUserId) {
            continue;
        }

        // Remove from the host map all hosts that are in a different user's
        // BER scheduling decision
        for (const auto& host : inFlightPair.second->hosts) {
            hostMap.erase(host);
        }
    }
}

std::vector<Host> CompactScheduler::getSortedHosts(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  const DecisionType& decisionType)
{
    std::vector<Host> sortedHosts;
    for (auto [ip, host] : hostMap) {
        sortedHosts.push_back(host);
    }

    std::shared_ptr<SchedulingDecision> oldDecision = nullptr;
    std::map<std::string, int> hostFreqCount;
    if (decisionType != DecisionType::NEW) {
        oldDecision = inFlightReqs.at(req->appid()).second;
        hostFreqCount = getHostFreqCount(oldDecision);
    }

    auto isFirstHostLarger = [&](const Host& hostA, const Host& hostB) -> bool {
        // The Compact scheduler sorts hosts by number of available slots
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

    auto isFirstHostLargerWithFreq = [&](auto hostA, auto hostB) -> bool {
        // When updating an existing scheduling decision (SCALE_CHANGE or
        // DIST_CHANGE), the BinPack scheduler takes into consideration the
        // existing host-message histogram (i.e. how many messages for this app
        // does each host _already_ run)

        int numInHostA = hostFreqCount.contains(getIp(hostA))
                           ? hostFreqCount.at(getIp(hostA))
                           : 0;
        int numInHostB = hostFreqCount.contains(getIp(hostB))
                           ? hostFreqCount.at(getIp(hostB))
                           : 0;

        // If at least one of the hosts has messages for this request, return
        // the host with the more messages for this request (note that it is
        // possible that this host has no available slots at all, in this case
        // we will just pack 0 messages here but we still want to sort it first
        // nontheless)
        if (numInHostA != numInHostB) {
            return numInHostA > numInHostB;
        }

        // In case of a tie, use the same criteria than NEW
        return isFirstHostLarger(hostA, hostB);
    };

    auto isFirstHostFuller = [&](const Host& hostA, const Host& hostB) -> bool {
        // In a DIST_CHANGE decision we want to globally minimise the
        // number of free VMs (i.e. COMPACT), so we sort the hosts in
        // increasing order of fullness and, in case of a tie, prefer hosts
        // that are already running messages for this app
        int nUsedA = numUsedSlots(hostA);
        int nUsedB = numUsedSlots(hostB);
        if (nUsedA != nUsedB) {
            return nUsedA > nUsedB;
        }

        // In case of a tie in free slots, prefer hosts already running
        // messages for this app
        // return isFirstHostLargerWithFreq(hostA, hostB);
        return isFirstHostLarger(hostA, hostB);
    };

    switch (decisionType) {
        case DecisionType::NEW: {
            // For a NEW decision type, the Compact scheduler just sorts the
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
            std::sort(sortedHosts.begin(),
                      sortedHosts.end(),
                      isFirstHostLargerWithFreq);
            break;
        }
        case DecisionType::DIST_CHANGE: {
            // When migrating, we want to know if the provided for app (which
            // is already in-flight) can be improved according to the compact
            // scheduling logic. This is equivalent to saying that the global
            // number of free hosts increases
            auto oldDecision = inFlightReqs.at(req->appid()).second;
            auto hostFreqCount = getHostFreqCount(oldDecision);

            // To decide on a migration opportunity, is like having another
            // shot at re-scheduling the app from scratch. Thus, we remove
            // the current slots we occupy, and try to fill in holes in the
            // existing host map

            // First remove the slots the app occupies to have a fresh new
            // shot at the scheduling
            for (auto host : sortedHosts) {
                if (hostFreqCount.contains(getIp(host))) {
                    freeSlots(host, hostFreqCount.at(getIp(host)));
                }
            }

            std::sort(
              sortedHosts.begin(), sortedHosts.end(), isFirstHostFuller);

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
std::shared_ptr<SchedulingDecision> CompactScheduler::makeSchedulingDecision(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<BatchExecuteRequest> req)
{
    auto decision = std::make_shared<SchedulingDecision>(req->appid(), 0);

    // Filter the hosts removing hosts that are executing tasks from different
    // users
    filterHosts(hostMap, inFlightReqs, req);

    // Get the sorted list of hosts
    auto decisionType = getDecisionType(inFlightReqs, req);
    auto sortedHosts = getSortedHosts(hostMap, inFlightReqs, req, decisionType);

    // Assign slots from the list (i.e. bin-pack)
    auto itr = sortedHosts.begin();
    int numLeftToSchedule = req->messages_size();
    int msgIdx = 0;
    while (itr < sortedHosts.end()) {
        // Calculate how many slots can we assign to this host (assign as many
        // as possible)
        int numOnThisHost =
          std::min<int>(numLeftToSchedule, numSlotsAvailable(*itr));
        for (int i = 0; i < numOnThisHost; i++) {
            decision->addMessage(getIp(*itr), req->messages(msgIdx));
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
        itr++;
    }

    // If we still have enough slots to schedule, we are out of slots
    if (numLeftToSchedule > 0) {
        return std::make_shared<SchedulingDecision>(NOT_ENOUGH_SLOTS_DECISION);
    }

    // In case of a DIST_CHANGE decision (i.e. migration), we want to make sure
    // that the new decision is better than the previous one
    if (decisionType == DecisionType::DIST_CHANGE) {
        auto oldDecision = inFlightReqs.at(req->appid()).second;
        if (isFirstDecisionBetter(hostMap, decision, oldDecision)) {
            // If we are sending a better migration, make sure that we minimise
            // the number of migrations to be done
            return minimiseNumOfMigrations(decision, oldDecision);
        }

        return std::make_shared<SchedulingDecision>(DO_NOT_MIGRATE_DECISION);
    }

    return decision;
}
}
