#include <faabric/planner/Planner.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

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

bool Planner::reset()
{
    SPDLOG_INFO("Resetting planner");

    flushHosts();

    return true;
}

bool Planner::flush(faabric::planner::FlushType flushType)
{
    switch (flushType) {
        case faabric::planner::FlushType::Hosts:
            SPDLOG_INFO("Planner flushing available hosts state");
            flushHosts();
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

std::vector<std::shared_ptr<Host>> Planner::getAvailableHosts()
{
    SPDLOG_DEBUG("Planner received request to get available hosts");

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
        state.hostMap.erase(host);
    }

    return availableHosts;
}

// Deliberately take a const reference as an argument to force a copy and take
// ownership of the host
bool Planner::registerHost(const Host& hostIn)
{
    SPDLOG_DEBUG("Planner received request to register host {}", hostIn.ip());

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
        SPDLOG_DEBUG("Registering host {}", hostIn.ip());
        state.hostMap.emplace(
          std::make_pair<std::string, std::shared_ptr<Host>>(
            (std::string)hostIn.ip(), std::make_shared<Host>(hostIn)));
    }

    // Irrespective, set the timestamp
    SPDLOG_DEBUG("Setting timestamp for host {}", hostIn.ip());
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

std::shared_ptr<faabric::util::SchedulingDecision> Planner::makeSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::util::SchedulingTopologyHint topologyHint)
{
    faabric::util::FullLock lock(plannerMx);

    if (req->messages_size() == 0) {
        SPDLOG_ERROR("Request has no messages!");
        // TODO: what to do here
        return nullptr;
    }

    int appId = req->messages().at(0).appid();
    int groupId = req->messages().at(0).groupid();

    // There are three types of scheduling of batch execute requests:
    // 1. New: first time we are scheduling the BER
    // 2. DistChange: we are requesting a change of distribution of an existing
    //    BER. This is indicated by requesting a scheduling decision for the
    //    same BER (same ID, and same number of messags).
    // 3. ScaleSchange: we are requesting a change of scale of an existing
    //    BER. This is indicated by providing the same BER
    // TODO: what about shared memory w/ repeated loops?
    auto decisionType = DecisionType::NO_DECISION_TYPE;
    auto it = state.inFlightRequests.find(appId);
    if (it == state.inFlightRequests.end()) {
        decisionType = DecisionType::NEW;
    } else if (it->second.first->messages_size() == req->messages_size()) {
        decisionType = DecisionType::DIST_CHANGE;
    } else {
        decisionType = DecisionType::SCALE_CHANGE;
    }

    // Helper methods used during scheduling
    auto nAvailable = [](std::shared_ptr<Host> host) -> int {
        return std::max<int>(host->slots() - host->usedslots(), 0);
    };
    auto claimSlots = [](std::shared_ptr<Host> host, int numClaimed) -> void {
        assert(host->usedslots() + numClaimed <= host->slots());
        host->set_usedslots(host->slots() + numClaimed);
    };
    auto isHostSmaller = [nAvailable](std::shared_ptr<Host> hostA, std::shared_ptr<Host> hostB) -> bool {
        // Sort hosts by number of free slots
        int availA = nAvailable(hostA);
        int availB = nAvailable(hostB);
        if (availA != availB) {
            return availA > availB;
        }

        // In case of tie, return the larger host first
        return hostA->slots() > hostB->slots();
    };

    auto availableHosts = doGetAvailableHosts();
    // TODO: should we check that none of the hosts in the existing decision
    // have become unavailble?
    switch (decisionType) {
        case DecisionType::NEW: {
            auto decision = std::make_shared<faabric::util::SchedulingDecision>(appId, groupId);

            // Get the sorted list of available hosts
            std::sort(availableHosts.begin(),
                      availableHosts.end(),
                      isHostSmaller);

            // Schedule requests to available hosts
            int numMessages = req->messages_size();
            int numLeftToSchedule = numMessages;
            for (auto& host : availableHosts) {
                // Work out how many messages go to this host
                int numOnThisHost = std::min<int>(nAvailable(host), numLeftToSchedule);

                // Update our records
                numLeftToSchedule -= numOnThisHost;
                claimSlots(host, numOnThisHost);
                for (int i = 0; i < numOnThisHost; i++) {
                    decision->plannerHosts.push_back(host);
                    decision->addMessage(host->ip(), req->messages().at(decision->plannerHosts.size() - 1));
                }
                if (numLeftToSchedule == 0) {
                    break;
                }
            }

            // Finally, add the request to in-flight and return the decision
            state.inFlightRequests[appId] = std::make_pair(req, decision);

            return decision;
        };
        case DecisionType::SCALE_CHANGE: {
            auto oldReq = state.inFlightRequests.at(appId).first;
            auto newReq = req;
            auto decision = state.inFlightRequests.at(appId).second;

            // If we are scaling down, make a new scheduling decision with the
            // remaining messages
            // TODO: think if we are ever gonna hit this
            if (oldReq->messages_size() > newReq->messages_size()) {
                SPDLOG_WARN("What do we do when we scale down?");
                return decision;
            }

            // If we are scaling up, we try to schedule messages first on hosts
            // that are already being used, second in new hosts. We assume that
            // when scaling up, all the messages in the old request are also
            // in the new request
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
                            bool isHostAKnown = hostFreqCount.find(hostA->ip()) != hostFreqCount.end();
                            bool isHostBKnown = hostFreqCount.find(hostB->ip()) != hostFreqCount.end();

                            if (isHostAKnown && isHostBKnown) {
                                if (hostFreqCount[hostA->ip()] != hostFreqCount[hostB->ip()]) {
                                    return hostFreqCount[hostA->ip()] > hostFreqCount[hostB->ip()];
                                }
                            } else if (isHostAKnown) {
                                // If host A is known but B is not, A is greater than B
                                return true;
                            } else if (isHostBKnown) {
                                // If host B is known but A is not, B is greater than A
                                return false;
                            }

                            return isHostSmaller(hostA, hostB);
                      });

            // Lastly, schedule requests to available hosts
            int numMessages = newReq->messages_size() - oldReq->messages_size();
            int numLeftToSchedule = numMessages;
            for (auto& host : sortedHosts) {
                // Work out how many messages go to this host
                int numOnThisHost = std::min<int>(nAvailable(host), numLeftToSchedule);

                // Update our records
                numLeftToSchedule -= numOnThisHost;
                claimSlots(host, numOnThisHost);
                for (int i = 0; i < numOnThisHost; i++) {
                    decision->plannerHosts.push_back(host);
                    decision->addMessage(host->ip(), req->messages().at(decision->plannerHosts.size() - 1));
                }
                if (numLeftToSchedule == 0) {
                    break;
                }
            }

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
                sortedHosts.push_back(std::make_pair(i, decision->plannerHosts.at(i)));
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
                            if (hostFreqCount[hostA->ip()] != hostFreqCount[hostB->ip()]) {
                                return hostFreqCount[hostA->ip()] > hostFreqCount[hostB->ip()];
                            }

                            return isHostSmaller(hostA, hostB);
                      });

            // So far, the only way we allow to change the distribution is by
            // consolidating to fewer hosts. We attempt to move messages from
            // lower frequency hosts (at the end of our sorted list), to higher
            // frequency hosts (at the begining in our sorted list)
            auto left = sortedHosts.begin();
            auto right = sortedHosts.end() - 1;
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
                claimSlots(left->second, 1);
                decision->plannerHosts.at(right->first) = left->second;
                decision->hosts.at(right->first) = left->second->ip();

                // Finally, move to a next candidate to be moved
                --right;
            }

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
    std::map<std::string, std::shared_ptr<faabric::BatchExecuteRequest>> hostRequests;

    {
        // Acquire a full-lock to populate the result promise map
        faabric::util::FullLock lock(plannerMx);

        for (int i = 0; i < req->messages_size(); i++) {
            auto msg = req->messages().at(i);
            // TODO: maybe assert differently?
            assert(msg.appid() == decision->appId);
            assert(msg.id() == decision->messageIds.at(i));
            assert(decision->hosts.at(i) == decision->plannerHosts.at(i)->ip());

            std::string thisHost = decision->hosts.at(i);
            if (hostRequests.find(thisHost) == hostRequests.end()) {
                hostRequests[thisHost] = faabric::util::batchExecFactory();
                hostRequests[thisHost]->set_snapshotkey(req->snapshotkey());
                hostRequests[thisHost]->set_type(req->type());
                hostRequests[thisHost]->set_subtype(req->subtype());
                hostRequests[thisHost]->set_contextdata(req->contextdata());
            }

            *hostRequests[thisHost]->add_messages() = msg;

            // Make sure the message result promise is registered
            state.appResults[msg.appid()][msg.id()];
        }
    }

    auto& sch = faabric::scheduler::getScheduler();
    for (const auto& [hostIp, hostReq] : hostRequests) {
        sch.getFunctionCallClient(hostIp)->executeFunctions(hostReq);
    }
}

bool Planner::waitForAppResult(std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    if (req->messages_size() == 0) {
        SPDLOG_ERROR("What shall we do here?");
    }

    int appId = req->messages().at(0).appid();

    // TODO: this is probably not going to work, as we may set the result
    // while we are waiting, and given that we take a copy here we will never
    // know

    // We acquire a lock just to get the sub-map corresponding to this app id
    std::map<int, std::promise<std::shared_ptr<faabric::Message>>> thisAppResults;
    {
        faabric::util::FullLock lock(plannerMx);

        if (state.appResults.find(appId) == state.appResults.end()) {
            SPDLOG_ERROR("Application {} not regsitered!", appId);
            return false;
        }

        thisAppResults = state.appResults[appId];
    }

    if (thisAppResults.size() != req->messages_size()) {
        SPDLOG_ERROR("Size mismtach between recorded app results and request: {} != {}", thisAppResults.size(), req->messages_size());
        return false;
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

void Planner::setMessageResult(std::shared_ptr<faabric::Message> msg)
{
    faabric::util::FullLock lock(plannerMx);

    int appId = msg->appid();
    int msgId = msg->id();

    if (state.appResults.find(appId) == state.appResults.end()) {
        SPDLOG_ERROR("App {} not registered in app results", appId);
        return;
    }

    if (state.appResults[appId].find(msgId) == state.appResults[appId].end()) {
        SPDLOG_ERROR("Msg {} not registered in app results (app id: {})", msgId, appId);
        return;
    }

    state.appResults[appId][msgId].set_value(std::make_shared<faabric::Message>(msg));
}

Planner& getPlanner()
{
    static Planner planner;
    return planner;
}
}
