#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/batch-scheduler/SpotScheduler.h>
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

bool SpotScheduler::isFirstDecisionBetter(
  std::shared_ptr<SchedulingDecision> decisionA,
  std::shared_ptr<SchedulingDecision> decisionB)
{
    throw std::runtime_error("Method not supported for COMPACT scheduler");
}

// Filter-out from the host map the next VM that will be evicted
static std::set<std::string> filterHosts(HostMap& hostMap)
{
    std::set<std::string> ipsToRemove;

    for (const auto& [hostIp, host] : hostMap) {
        if (host->ip == MUST_EVICT_IP) {
            ipsToRemove.insert(hostIp);
        }
    }

    for (const auto& ipToRemove : ipsToRemove) {
        hostMap.erase(ipToRemove);
    }

    return ipsToRemove;
}

std::vector<Host> SpotScheduler::getSortedHosts(
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
        // The SPOT scheduler sorts hosts by number of available slots
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
        // DIST_CHANGE), the SPOT scheduler takes into consideration the
        // existing host-message histogram (i.e. how many messages for this app
        // does each host _already_ run). This behaviour is the same than the
        // BIN_PACK and COMPACT policies

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

    switch (decisionType) {
        case DecisionType::NEW: {
            // For a NEW decision type, the SPOT scheduler just sorts the
            // hosts in decreasing order of capacity, and bin-packs messages
            // to hosts in this order. This has one caveat that it skips the
            // next VM that we know will be evicted
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
            // A DIST_CHANGE with the SPOT scheduler means that, if the app
            // is running any messages of the to-be-evicted VM, we must move
            // them from there. Two things may happen:
            // * We have slots to move them-to (equivalent to re-scheduling
            //   from scratch without the tainted VM)
            // * We do not have slots to move them-to, in which case all
            //   messages need to freeze until there is capacity in the cluster
            //   again

            auto oldDecision = inFlightReqs.at(req->appid()).second;
            auto hostFreqCount = getHostFreqCount(oldDecision);

            // First remove the slots the app occupies to have a fresh new
            // shot at the scheduling
            for (auto host : sortedHosts) {
                if (hostFreqCount.contains(getIp(host))) {
                    freeSlots(host, hostFreqCount.at(getIp(host)));
                }
            }

            // Try to schedule again without the tainted VM. Note that this
            // app may not be using the tainted VM _at all_ in which case we
            // will just discard the suggested migration.
            std::sort(sortedHosts.begin(),
                      sortedHosts.end(),
                      isFirstHostLargerWithFreq);

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
// messages to schedule. The SPOT scheduler behaves as the BinPack for
// NEW and SCALE_CHANGE requests, with two caveats:
// - it avoids setting any messages to a host that is going to be evicted
// - when migrating, it will check if the migration candidate has any messages
//   running in the to-be-evicted VM. If so, it will try to migrate messages
//   away from the evicted-to-VM. If it cannot, it will request the app to
//   INTERRUPT
std::shared_ptr<SchedulingDecision> SpotScheduler::makeSchedulingDecision(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<BatchExecuteRequest> req)
{
    auto decision = std::make_shared<SchedulingDecision>(req->appid(), 0);

    // Filter the hosts removing the VM that will be evicted next
    std::set<std::string> evictedHostIps = filterHosts(hostMap);

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

    bool isDistChange = decisionType == DecisionType::DIST_CHANGE;

    // If we still have enough slots to schedule, we are out of slots
    if (numLeftToSchedule > 0 && !isDistChange) {
        return std::make_shared<SchedulingDecision>(NOT_ENOUGH_SLOTS_DECISION);
    }

    if (isDistChange) {
        // If we ran out of slots whilst processing a migration request it
        // means that we have some messages running in the to-be-evicted VM
        // and we can not migrate them elsewhere. In this case we must FREEZE
        // all messages
        if (numLeftToSchedule > 0) {
            return std::make_shared<SchedulingDecision>(MUST_FREEZE_DECISION);
        }

        // Check if we are running any messages in the to-be evicted VM. Only
        // migrate if we are
        auto oldDecision = inFlightReqs.at(req->appid()).second;
        for (const auto& hostIp : oldDecision->hosts) {
            if (evictedHostIps.contains(hostIp)) {
                // If we are requesting a migration, make sure that we minimise
                // the number of messages to actuall migrate
                return minimiseNumOfMigrations(decision, oldDecision);
            }
        }

        return std::make_shared<SchedulingDecision>(DO_NOT_MIGRATE_DECISION);
    }

    return decision;
}
}
