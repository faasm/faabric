#include <faabric/planner/Planner.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/MessageResultPromise.h>
#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>

#include <string>

namespace faabric::planner {
// Planner is used globally as a static variable. This constructor relies on
// the fact that C++ static variable's initialisation is thread-safe
Planner::Planner()
{
    // Note that we don't initialise the config in a separate method to prevent
    // that method from being called elsewhere in the codebase (as it would be
    // thread-unsafe)
    config.set_ip(faabric::util::getSystemConfig().endpointHost);
    config.set_hosttimeout(std::stoi(
      faabric::util::getEnvVar("PLANNER_HOST_KEEPALIVE_TIMEOUT", "5")));
    config.set_numthreadshttpserver(
      std::stoi(faabric::util::getEnvVar("PLANNER_HTTP_SERVER_THREADS", "4")));

    printConfig();
}

PlannerConfig Planner::getConfig()
{
    return config;
}

void Planner::printConfig() const
{
    SPDLOG_INFO("--- Planner Conifg ---");
    SPDLOG_INFO("HOST_KEEP_ALIVE_TIMEOUT    {}", config.hosttimeout());
    SPDLOG_INFO("HTTP_SERVER_THREADS        {}", config.numthreadshttpserver());
}

void Planner::setTestsConfig(PlannerTestsConfig& testsConfigIn, bool reset)
{
    SPDLOG_INFO("Planner setting configuration for tests");

    isTestMode.store(!reset, std::memory_order_release);

    faabric::util::FullLock lock(plannerMx);

    testsConfig = testsConfigIn;
}

bool Planner::reset()
{
    SPDLOG_INFO("Resetting planner");

    flushHosts();

    flushSchedulingState();

    PlannerTestsConfig emptyConfig;
    setTestsConfig(emptyConfig, true);

    return true;
}

bool Planner::flush(faabric::planner::FlushType flushType)
{
    // TODO: flush scheduling state
    // TODO: flush results
    switch (flushType) {
        case faabric::planner::FlushType::Hosts:
            SPDLOG_INFO("Planner flushing available hosts state");
            flushHosts();
            return true;
        case faabric::planner::FlushType::Executors:
            SPDLOG_INFO("Planner flushing executors");
            flushExecutors();
            return true;
        default:
            SPDLOG_ERROR("Unrecognised flush type");
            return false;
    }
}

void Planner::flushHosts()
{
    faabric::util::FullLock lock(plannerMx);

    state.hostMap.clear();
}

void Planner::flushExecutors()
{
    auto availableHosts = getAvailableHosts();

    auto& sch = faabric::scheduler::getScheduler();
    for (const auto& host : availableHosts) {
        SPDLOG_INFO("Planner sending EXECUTOR flush to {}", host->ip());
        sch.getFunctionCallClient(host->ip())->sendFlush();
    }
}

void Planner::flushSchedulingState()
{
    faabric::util::FullLock lock(plannerMx);

    state.inFlightRequests.clear();
    state.appResults.clear();
    state.appResultWaiters.clear();
}

std::vector<std::shared_ptr<Host>> Planner::getAvailableHosts()
{
    // Acquire a full lock because we will also remove the hosts that have
    // timed out
    faabric::util::FullLock lock(plannerMx);

    return doGetAvailableHosts();
}

std::vector<std::shared_ptr<Host>> Planner::doGetAvailableHosts()
{
    std::vector<std::string> hostsToRemove;
    std::vector<std::shared_ptr<Host>> availableHosts;
    auto timeNowMs = faabric::util::getGlobalClock().epochMillis();
    for (const auto& [ip, host] : state.hostMap) {
        if (isHostExpired(host, timeNowMs)) {
            hostsToRemove.push_back(ip);
        } else {
            availableHosts.push_back(host);
        }
    }

    for (const auto& host : hostsToRemove) {
        SPDLOG_INFO("Removing host {} after not receiving keep-alive", host);
        state.hostMap.erase(host);
    }

    return availableHosts;
}

// Deliberately take a const reference as an argument to force a copy and take
// ownership of the host
bool Planner::registerHost(const Host& hostIn)
{
    SPDLOG_TRACE("Planner received request to register host {}", hostIn.ip());

    // Sanity check the input argument
    if (hostIn.slots() < 0) {
        SPDLOG_ERROR(
          "Received erroneous request to register host {} with {} slots",
          hostIn.ip(),
          hostIn.slots());
        return false;
    }

    faabric::util::FullLock lock(plannerMx);

    auto it = state.hostMap.find(hostIn.ip());
    if (it == state.hostMap.end() || isHostExpired(it->second)) {
        // If the host entry has expired, we remove it and treat the host
        // as a new one
        if (it != state.hostMap.end()) {
            state.hostMap.erase(it);
        }

        // If its the first time we see this IP, give it a UID and add it to
        // the map
        SPDLOG_DEBUG(
          "Registering host {} with {} slots", hostIn.ip(), hostIn.slots());
        state.hostMap.emplace(
          std::make_pair<std::string, std::shared_ptr<Host>>(
            (std::string)hostIn.ip(), std::make_shared<Host>(hostIn)));
    } else if (it != state.hostMap.end() &&
               ((it->second->slots() != hostIn.slots()) ||
                (it->second->usedslots() != hostIn.usedslots()))) {
        // We allow overwritting the host state by sending another register
        // request with same IP but different host resources. This is useful
        // for testing and resetting purposes
        SPDLOG_DEBUG("Overwritting host {} with {} slots (used {})",
                     hostIn.ip(),
                     hostIn.slots(),
                     hostIn.usedslots());
        it->second->set_slots(hostIn.slots());
        it->second->set_usedslots(hostIn.usedslots());
    }

    // Irrespective, set the timestamp
    SPDLOG_TRACE("Setting timestamp for host {}", hostIn.ip());
    state.hostMap.at(hostIn.ip())
      ->mutable_registerts()
      ->set_epochms(faabric::util::getGlobalClock().epochMillis());

    return true;
}

void Planner::removeHost(const Host& hostIn)
{
    SPDLOG_DEBUG("Planner received request to remove host {}", hostIn.ip());

    // We could acquire first a read lock to see if the host is in the host
    // map, and then acquire a write lock to remove it, but we don't do it
    // as we don't expect that much throughput in the planner
    faabric::util::FullLock lock(plannerMx);

    auto it = state.hostMap.find(hostIn.ip());
    if (it != state.hostMap.end()) {
        SPDLOG_DEBUG("Planner removing host {}", hostIn.ip());
        state.hostMap.erase(it);
    }
}

bool Planner::isHostExpired(std::shared_ptr<Host> host, long epochTimeMs)
{
    // Allow calling the method without a timestamp, and we calculate it now
    if (epochTimeMs == 0) {
        epochTimeMs = faabric::util::getGlobalClock().epochMillis();
    }

    long hostTimeoutMs = getConfig().hosttimeout() * 1000;
    return (epochTimeMs - host->registerts().epochms()) > hostTimeoutMs;
}

// ----------
// Request scheduling
// ----------

std::shared_ptr<faabric::util::SchedulingDecision>
Planner::makeSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::util::SchedulingTopologyHint topologyHint)
{
    faabric::util::FullLock lock(plannerMx);

    int appId = req->appid();

    // There are three types of scheduling of batch execute requests:
    // 1. New: first time we are scheduling the BER
    // 2. DistChange: we are requesting a change of distribution of an existing
    //    BER. This is indicated by requesting a scheduling decision for the
    //    same BER (same ID, and same number of messags).
    // 3. ScaleSchange: we are requesting a change of scale of an existing
    //    BER. This is indicated by providing the same BER
    // TODO: what about shared memory w/ repeated loops?
    // TODO: simplify this and use a field in the actual request
    auto decisionType = DecisionType::NO_DECISION_TYPE;
    auto it = state.inFlightRequests.find(appId);
    if (it == state.inFlightRequests.end()) {
        decisionType = DecisionType::NEW;
        SPDLOG_INFO("Planner making NEW scheduling decision for app {}", appId);
    } else {
        if (req->type() == faabric::BatchExecuteRequest::DIST_CHANGE) {
            decisionType = DecisionType::DIST_CHANGE;
            SPDLOG_INFO("Planner deciding DIST_CHANGE for app {}", appId);
        } else {
            decisionType = DecisionType::SCALE_CHANGE;
            SPDLOG_INFO("Planner deciding SCALE_CHANGE for app {} ({} -> {})",
                        appId,
                        it->second.first->messages_size(),
                        it->second.first->messages_size() +
                          req->messages_size());
        }
    }

    // Helper methods used during scheduling
    auto nAvailable = [](std::shared_ptr<Host> host) -> int {
        return std::max<int>(host->slots() - host->usedslots(), 0);
    };
    auto claimSlots = [](std::shared_ptr<Host> host, int numClaimed) -> void {
        assert(host->usedslots() + numClaimed <= host->slots());
        host->set_usedslots(host->usedslots() + numClaimed);
    };
    // This function is used as a predicate to std::sort. Being smaller means
    // going first in the sorted list
    auto isHostSmaller = [nAvailable](std::shared_ptr<Host> hostA,
                                      std::shared_ptr<Host> hostB) -> bool {
        // Sort hosts by number of free slots
        int availA = nAvailable(hostA);
        int availB = nAvailable(hostB);
        if (availA != availB) {
            return availA > availB;
        }

        // In case of tie, return the larger host first
        if (hostA->slots() != hostB->slots()) {
            return hostA->slots() > hostB->slots();
        }

        // Lastly, in case of tie again, return the largest alphabetically.
        // This is useful in the tests to make sure LOCALHOST is last in case
        // of mocking
        return hostA->ip() > hostB->ip();
    };

    // Set the list of hosts to skip when sending messages. This is used for
    // mocking in the tests
    // TODO: do a different mocking in the tests
    std::set<std::string> hostSkipList;
    if (isTestMode.load(std::memory_order_acquire)) {
        for (const auto& host : testsConfig.mockedhosts()) {
            hostSkipList.insert(host);
        }
    }

    // TODO: should we check that none of the hosts in the existing decision
    // have become unavailble?
    auto availableHosts = doGetAvailableHosts();
    int numFreeSlots = 0;
    for (const auto& host : availableHosts) {
        numFreeSlots += nAvailable(host);
    }

    // A scheduling decision will generate a new PTP mapping, and therefore
    // a new group ID
    int groupId = faabric::util::generateGid();
    req->set_groupid(groupId);

    auto& broker = faabric::transport::getPointToPointBroker();
    switch (decisionType) {
        case DecisionType::NEW: {
            // Fail-fast if we don't have enough capacity
            int numMessages = req->messages_size();
            int numLeftToSchedule = numMessages;
            if (numFreeSlots < numMessages) {
                SPDLOG_ERROR("Not enough capacity to schedule request {}",
                             appId);
                SPDLOG_ERROR(
                  "Requested {} messages, but have only {} free slots",
                  numMessages,
                  numFreeSlots);
                return nullptr;
            }

            auto decision = std::make_shared<faabric::util::SchedulingDecision>(
              appId, groupId);

            // Get the sorted list of available hosts
            std::sort(
              availableHosts.begin(), availableHosts.end(), isHostSmaller);

            // Schedule requests to available hosts
            for (auto& host : availableHosts) {
                // Work out how many messages go to this host
                int numOnThisHost =
                  std::min<int>(nAvailable(host), numLeftToSchedule);

                // Update our records
                numLeftToSchedule -= numOnThisHost;
                claimSlots(host, numOnThisHost);
                for (int i = 0; i < numOnThisHost; i++) {
                    decision->plannerHosts.push_back(host);
                    decision->addMessage(
                      host->ip(),
                      req->messages().at(decision->plannerHosts.size() - 1));
                    req->mutable_messages(decision->plannerHosts.size() - 1)
                      ->set_groupid(groupId);
                }
                if (numLeftToSchedule == 0) {
                    break;
                }
            }

            if (numLeftToSchedule != 0) {
                SPDLOG_ERROR("Ran out of available hosts with {} messages left "
                             "to schedule!",
                             numLeftToSchedule);
                return nullptr;
            }

            // Add the request to in-flight
            state.inFlightRequests[appId] = std::make_pair(req, decision);

            broker.setAndSendMappingsFromSchedulingDecision(*decision,
                                                            hostSkipList);

            return decision;
        };
        case DecisionType::SCALE_CHANGE: {
            // TODO: shall we set the groupidx here instead of relying on the
            // calling function?
            auto oldReq = state.inFlightRequests.at(appId).first;
            auto newReq = req;
            auto decision = state.inFlightRequests.at(appId).second;

            int numMessages = req->messages_size();
            if (numFreeSlots < numMessages) {
                SPDLOG_ERROR("Not enough capacity to schedule app {}:", appId);
                SPDLOG_ERROR(
                  "requested {} messages, but have only {} free slots",
                  numMessages,
                  numFreeSlots);
                return nullptr;
            }

            // Set the new group ID after the scale change
            oldReq->set_groupid(groupId);
            decision->groupId = groupId;

            // If we are scaling up, we try to schedule messages first on hosts
            // that are already being used, second in new hosts. We assume that
            // when scaling up, the messages in the old request are _not_ in
            // the new request
            std::map<std::string, int> hostFreqCount;
            auto sortedHosts = decision->plannerHosts;
            for (const auto& host : sortedHosts) {
                hostFreqCount[host->ip()] += 1;
            }
            for (const auto& host : availableHosts) {
                if (hostFreqCount.find(host->ip()) == hostFreqCount.end()) {
                    sortedHosts.push_back(host);
                }
            }
            // TODO: maybe we don't need to complicate this too much
            std::sort(sortedHosts.begin(),
                      sortedHosts.end(),
                      [&](auto hostA, auto hostB) {
                          bool isHostAKnown = hostFreqCount.find(hostA->ip()) !=
                                              hostFreqCount.end();
                          bool isHostBKnown = hostFreqCount.find(hostB->ip()) !=
                                              hostFreqCount.end();

                          if (isHostAKnown && isHostBKnown) {
                              if (hostFreqCount[hostA->ip()] !=
                                  hostFreqCount[hostB->ip()]) {
                                  return hostFreqCount[hostA->ip()] >
                                         hostFreqCount[hostB->ip()];
                              }
                          } else if (isHostAKnown) {
                              // If host A is known but B is not, A is greater
                              // than B
                              return true;
                          } else if (isHostBKnown) {
                              // If host B is known but A is not, B is greater
                              // than A
                              return false;
                          }

                          return isHostSmaller(hostA, hostB);
                      });

            // Lastly, schedule requests to available hosts
            int numLeftToSchedule = numMessages;
            // This index goes from 0 to req->messages_size() - 1
            int nextMessageToScheduleIdx = 0;
            for (auto& host : sortedHosts) {
                // Work out how many messages go to this host
                int numOnThisHost =
                  std::min<int>(nAvailable(host), numLeftToSchedule);

                // Update our records
                numLeftToSchedule -= numOnThisHost;
                claimSlots(host, numOnThisHost);
                for (int i = 0; i < numOnThisHost; i++) {
                    // Update the in-flight map
                    // First, update the group id on the message
                    req->mutable_messages(nextMessageToScheduleIdx)
                      ->set_groupid(groupId);
                    // Second, update the scheduling decision
                    decision->plannerHosts.push_back(host);
                    decision->addMessage(
                      host->ip(), req->messages().at(nextMessageToScheduleIdx));
                    // Last, update the BatchExecuteRequest
                    *oldReq->add_messages() =
                      *req->mutable_messages(nextMessageToScheduleIdx);

                    // Update the index of the next message to schedule
                    ++nextMessageToScheduleIdx;
                }
                if (numLeftToSchedule == 0) {
                    break;
                }
            }

            broker.setAndSendMappingsFromSchedulingDecision(*decision,
                                                            hostSkipList);
            return decision;
        };
        case DecisionType::DIST_CHANGE: {
            auto decision = state.inFlightRequests.at(appId).second;

            // First, sort the existing hosts in the scheduling decision by
            // frequency. Note that we also need to keep track of the original
            // index to update the decision in case we change the distribution
            std::map<std::string, int> hostFreqCount;
            std::vector<std::pair<int, std::shared_ptr<Host>>> sortedHosts;
            for (int i = 0; i < decision->plannerHosts.size(); i++) {
                sortedHosts.push_back(
                  std::make_pair(i, decision->plannerHosts.at(i)));
                hostFreqCount[decision->plannerHosts.at(i)->ip()] += 1;
            }
            std::sort(sortedHosts.begin(),
                      sortedHosts.end(),
                      [&](auto pairA, auto pairB) {
                          auto hostA = pairA.second;
                          auto hostB = pairB.second;
                          // Sort first the host with the highest frequency
                          // count. I.e. the host that is already executing
                          // the largest number of messages for this request
                          if (hostFreqCount[hostA->ip()] !=
                              hostFreqCount[hostB->ip()]) {
                              return hostFreqCount[hostA->ip()] >
                                     hostFreqCount[hostB->ip()];
                          }

                          return isHostSmaller(hostA, hostB);
                      });

            // So far, the only way we allow to change the distribution is by
            // consolidating to fewer hosts. We attempt to move messages from
            // lower frequency hosts (at the end of our sorted list), to higher
            // frequency hosts (at the begining in our sorted list)
            auto left = sortedHosts.begin();
            auto right = sortedHosts.end() - 1;
            bool hasDistChanged = false;
            std::set<std::string> oldHostIps(decision->hosts.begin(),
                                             decision->hosts.end());
            while (left < right) {
                // If both pointers point to the same host, check another
                // possible candidate to be moved
                if (left->second->ip() == right->second->ip()) {
                    --right;
                    continue;
                }

                // If there's no space in the possible destination, move to the
                // next possible destination
                if (nAvailable(left->second) == 0) {
                    ++left;
                    continue;
                }

                // If both pointers point to different hosts, and the
                // destination has enough slots, update our records
                hasDistChanged = true;
                SPDLOG_DEBUG("Migrating group idx {} from {} to {}",
                             decision->groupIdxs.at(right->first),
                             right->second->ip(),
                             left->second->ip());
                claimSlots(left->second, 1);
                decision->plannerHosts.at(right->first) = left->second;
                decision->hosts.at(right->first) = left->second->ip();

                // Finally, move to a next candidate to be moved
                --right;
            }

            if (hasDistChanged) {
                req->set_groupid(groupId);
                decision->groupId = groupId;
            }

            // Make sure hosts that are not part of the _new_ scheduling
            // decision also get the mappings
            std::set<std::string> newHostIps(decision->hosts.begin(),
                                             decision->hosts.end());
            std::set<std::string> evictedHosts;
            std::set_difference(
              oldHostIps.begin(),
              oldHostIps.end(),
              newHostIps.begin(),
              newHostIps.end(),
              std::inserter(evictedHosts, evictedHosts.begin()));

            // Send the mappings to the hosts involved in the new decision
            broker.setAndSendMappingsFromSchedulingDecision(*decision,
                                                            hostSkipList);
            // Also send the mappings to the hosts that will be emptied as
            // a consequence of the migration
            broker.sendMappingsFromSchedulingDecision(
              *decision, evictedHosts, hostSkipList);
            return decision;
        };
        case DecisionType::NO_DECISION_TYPE:
        default:
            SPDLOG_ERROR("We should not be here!");
            // TODO: don't throw here
            throw std::runtime_error("Unreachable");
    };
}

void Planner::dispatchSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  std::shared_ptr<faabric::util::SchedulingDecision> decision)
{
    std::map<std::string, std::shared_ptr<faabric::BatchExecuteRequest>>
      hostRequests;

    // Note that the just-scheduled messages in the request will correspond to
    // the last req->messages_size() messages in the scheduling decision
    int totalAppMessages = decision->hosts.size();
    int newAppMessages = req->messages_size();
    int j = totalAppMessages - newAppMessages;
    for (int i = 0; i < newAppMessages; i++, j++) {
        auto msg = req->messages().at(i);
        // TODO: maybe assert differently to prevent the planner from dying
        // if something goes wrong. Maybe we want it to die?
        assert(msg.appid() == decision->appId);
        assert(msg.groupid() == decision->groupId);
        assert(msg.id() == decision->messageIds.at(j));
        assert(msg.groupidx() == decision->groupIdxs.at(j));
        assert(decision->hosts.at(j) == decision->plannerHosts.at(j)->ip());

        std::string thisHost = decision->hosts.at(j);
        if (hostRequests.find(thisHost) == hostRequests.end()) {
            hostRequests[thisHost] = faabric::util::batchExecFactory();
            hostRequests[thisHost]->set_appid(decision->appId);
            hostRequests[thisHost]->set_groupid(decision->groupId);
            hostRequests[thisHost]->set_snapshotkey(req->snapshotkey());
            hostRequests[thisHost]->set_type(req->type());
            hostRequests[thisHost]->set_subtype(req->subtype());
            hostRequests[thisHost]->set_contextdata(req->contextdata());
        }

        *hostRequests[thisHost]->add_messages() = msg;
    }

    // In tests, remove the hosts that we are mocking
    if (isTestMode.load(std::memory_order_acquire)) {
        faabric::util::SharedLock lock(plannerMx);

        for (const auto& hostIp : testsConfig.mockedhosts()) {
            auto it = hostRequests.find(hostIp);
            if (it != hostRequests.end()) {
                hostRequests.erase(it);
                SPDLOG_DEBUG("Skipping dispatching messages to {}", hostIp);
            }
        }
    }

    auto& sch = faabric::scheduler::getScheduler();
    for (const auto& [hostIp, hostReq] : hostRequests) {
        SPDLOG_DEBUG("Dispatching {} messages to host {} for execution",
                     hostReq->messages_size(),
                     hostIp);
        sch.getFunctionCallClient(hostIp)->executeFunctions(hostReq);
    }

    SPDLOG_INFO("Finished dispatching {} messages for execution",
                req->messages_size());
}

std::shared_ptr<faabric::util::SchedulingDecision>
Planner::getSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    int appId = req->appid();
    SPDLOG_INFO("Planner getting scheduling decision for app: {}", appId);

    faabric::util::SharedLock lock(plannerMx);

    if (state.inFlightRequests.find(appId) == state.inFlightRequests.end()) {
        return nullptr;
    }

    return state.inFlightRequests.at(appId).second;
}

void Planner::setMessageResult(std::shared_ptr<faabric::Message> msg)
{
    int appId = msg->appid();
    int msgId = msg->id();

    faabric::util::FullLock lock(plannerMx);

    if (state.inFlightRequests.find(appId) == state.inFlightRequests.end()) {
        SPDLOG_ERROR("Did not find app {} in in-flight requests", appId);
        return;
    }

    SPDLOG_INFO("Planner setting message result (id: {}) for {}:{}:{}",
                msg->id(),
                msg->appid(),
                msg->groupid(),
                msg->groupidx());

    // Free the resources corresponding to this message from the in-flight map
    auto inFlightDec = state.inFlightRequests.at(appId).second;
    try {
        // Note that this call also releases the used slot
        inFlightDec->removeMessage(*msg);
    } catch (std::exception& e) {
        SPDLOG_ERROR(
          "Error removing message {} from scheduling decision (app id: {}): {}",
          msg->id(),
          msg->appid(),
          e.what());
        return;
    }
    auto inFlightReq = state.inFlightRequests.at(appId).first;
    auto it =
      std::find_if(inFlightReq->messages().begin(),
                   inFlightReq->messages().end(),
                   [&](auto innerMsg) { return innerMsg.id() == msg->id(); });
    if (it == inFlightReq->messages().end()) {
        SPDLOG_ERROR(
          "Error removing message {} from in-flight batch request (app id: {})",
          msg->id(),
          msg->appid());
        return;
    }
    inFlightReq->mutable_messages()->erase(it);

    if (inFlightReq->messages_size() == 0) {
        SPDLOG_INFO("Planner removing app {} from in-flight", appId);
        state.inFlightRequests.erase(appId);
    }

    // Set the result
    state.appResults[appId][msgId] = msg;

    // Dispatch an async message to all hosts that are waiting
    auto& sch = faabric::scheduler::getScheduler();
    if (state.appResultWaiters.find(msgId) != state.appResultWaiters.end()) {
        for (const auto& host : state.appResultWaiters[msgId]) {
            sch.getFunctionCallClient(host)->setMessageResult(msg);
        }
    }
}

std::shared_ptr<faabric::Message> Planner::getMessageResult(
  std::shared_ptr<faabric::Message> msg)
{
    int appId = msg->appid();
    int msgId = msg->id();

    {
        faabric::util::SharedLock lock(plannerMx);

        if (state.appResults.find(appId) == state.appResults.end()) {
            SPDLOG_ERROR("App {} not registered in app results", appId);
        } else if (state.appResults[appId].find(msgId) == state.appResults[appId].end()) {
            SPDLOG_ERROR(
              "Msg {} not registered in app results (app id: {})", msgId, appId);
        } else {
            return state.appResults[appId][msgId];
        }
    }

    // If we are here, it means that we have not found the message result, so
    // we register the calling-host's interest
    {
        faabric::util::FullLock lock(plannerMx);
        SPDLOG_DEBUG("Adding host {} on the waiting list for message {}",
                     msg->masterhost(),
                     msgId);
        state.appResultWaiters[msgId].push_back(msg->masterhost());
    }

    return nullptr;
}

/* TODO: finish me!
void Planner::getMessageResultAsync(
  std::shared_ptr<faabric::Message> msg,
  int timeoutMs,
  asio::io_context& ioc,
  asio::any_io_executor& executor,
  std::function<void(std::shared_ptr<faabric::Message>)> handler)
{
    int appId = msg->appid();
    int msgId = msg->id();

    while (true) {
        auto mrp = getMessageResult(msg);

        auto awaiter =
std::make_shared<faabric::util::MessageResultPromiseAwaiter>( msg, this, mrp,
          asio::posix::stream_descriptor(ioc, mrp->eventFd),
          std::move(handler));
        awaiter->doAwait();
        return;
    }
}
*/

// This method returns all the registered messages for the calling requests'
// app id. We combine the in-flight messages and the result ones
std::shared_ptr<faabric::BatchExecuteRequest> Planner::getBatchMessages(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    int appId = req->appid();
    auto responseReq = req;
    responseReq->clear_messages();

    SPDLOG_INFO("Planner getting batch result for app: {}", appId);

    faabric::util::SharedLock lock(plannerMx);

    auto inFlightIt = state.inFlightRequests.find(appId);
    if (inFlightIt != state.inFlightRequests.end()) {
        auto inFlightReq = inFlightIt->second.first;
        for (auto& msg : *inFlightReq->mutable_messages()) {
            *responseReq->add_messages() = msg;
        }
    }

    auto resultsIt = state.appResults.find(appId);
    if (resultsIt != state.appResults.end()) {
        for (auto& msg : resultsIt->second) {
            *responseReq->add_messages() = *(msg.second);
        }
    }

    return responseReq;
}

/*
bool Planner::waitForAppResult(std::shared_ptr<faabric::BatchExecuteRequest>
req)
{
    if (req->messages_size() == 0) {
        SPDLOG_ERROR("What shall we do here?");
    }

    int appId = req->messages().at(0).appid();

    // TODO: this is probably not going to work, as we may set the result
    // while we are waiting, and given that we take a copy here we will never
    // know

    // We acquire a lock just to get the sub-map corresponding to this app id
    std::map<int, std::promise<std::shared_ptr<faabric::Message>>>
thisAppResults;
    {
        faabric::util::FullLock lock(plannerMx);

        if (state.appResults.find(appId) == state.appResults.end()) {
            SPDLOG_ERROR("Application {} not regsitered!", appId);
            return false;
        }

        thisAppResults = state.appResults[appId];
    }

    if (thisAppResults.size() != req->messages_size()) {
        SPDLOG_ERROR("Size mismtach between recorded app results and request: {}
!= {}", thisAppResults.size(), req->messages_size()); return false;
    }

    std::vector<std::shared_ptr<faabric::Message>> resultMsgs;
    for (int i = 0; i < req->messages_size(); i++) {
        auto* msg = req->mutable_messages(i);
        if (thisAppResults.find(msg->id()) == thisAppResults.end()) {
            SPDLOG_ERROR("Message result {} not registered!", msg->id());
            continue;
        }

        SPDLOG_INFO("Waiting for result {} (app id: {})", msg->id(), appId);
        auto newMsgPtr = thisAppResults[msg->id()].get_future().get();
        *msg = *newMsgPtr;
    }

    return true;
}
*/

Planner& getPlanner()
{
    static Planner planner;
    return planner;
}
}
