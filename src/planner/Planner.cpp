#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/planner/Planner.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/batch.h>
#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
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

    for (const auto& host : availableHosts) {
        SPDLOG_INFO("Planner sending EXECUTOR flush to {}", host->ip());
        faabric::scheduler::getFunctionCallClient(host->ip())->sendFlush();
    }
}

std::vector<std::shared_ptr<Host>> Planner::getAvailableHosts()
{
    SPDLOG_DEBUG("Planner received request to get available hosts");

    // Acquire a full lock because we will also remove the hosts that have
    // timed out
    faabric::util::FullLock lock(plannerMx);

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
bool Planner::registerHost(const Host& hostIn, bool overwrite)
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
        SPDLOG_INFO(
          "Registering host {} with {} slots", hostIn.ip(), hostIn.slots());
        state.hostMap.emplace(
          std::make_pair<std::string, std::shared_ptr<Host>>(
            (std::string)hostIn.ip(), std::make_shared<Host>(hostIn)));
    } else if (it != state.hostMap.end() && overwrite) {
        // We allow overwritting the host state by sending another register
        // request with same IP but different host resources. This is useful
        // for testing and resetting purposes
        SPDLOG_INFO("Overwritting host {} with {} slots (used {})",
                    hostIn.ip(),
                    hostIn.slots(),
                    hostIn.usedslots());
        it->second->set_slots(hostIn.slots());
        it->second->set_usedslots(hostIn.usedslots());
    } else if (it != state.hostMap.end()) {
        SPDLOG_TRACE("NOT overwritting host {} with {} slots (used {})",
                     hostIn.ip(),
                     hostIn.slots(),
                     hostIn.usedslots());
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

void Planner::setMessageResult(std::shared_ptr<faabric::Message> msg)
{
    int appId = msg->appid();
    int msgId = msg->id();

    faabric::util::FullLock lock(plannerMx);

    SPDLOG_INFO("Planner setting message result (id: {}) for {}:{}:{}",
                msg->id(),
                msg->appid(),
                msg->groupid(),
                msg->groupidx());

    // Set the result
    state.appResults[appId][msgId] = msg;

    // Dispatch an async message to all hosts that are waiting
    if (state.appResultWaiters.find(msgId) != state.appResultWaiters.end()) {
        for (const auto& host : state.appResultWaiters[msgId]) {
            SPDLOG_INFO("Sending result to waiting host: {}", host);
            faabric::scheduler::getFunctionCallClient(host)->setMessageResult(
              msg);
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

        // We debug and not error these messages as they happen frequently
        // when polling for results
        if (state.appResults.find(appId) == state.appResults.end()) {
            SPDLOG_DEBUG("App {} not registered in app results", appId);
        } else if (state.appResults[appId].find(msgId) ==
                   state.appResults[appId].end()) {
            SPDLOG_DEBUG("Msg {} not registered in app results (app id: {})",
                         msgId,
                         appId);
        } else {
            return state.appResults[appId][msgId];
        }
    }

    // If we are here, it means that we have not found the message result, so
    // we register the calling-host's interest if the calling-host has
    // provided a main host. The main host is set when dispatching a message
    // within faabric, but not when sending an HTTP request
    if (!msg->mainhost().empty()) {
        faabric::util::FullLock lock(plannerMx);

        // Check again if the result is not set, as it could have been set
        // between releasing the shared lock and acquiring the full lock
        if (state.appResults.contains(appId) &&
            state.appResults[appId].contains(msgId)) {
            return state.appResults[appId][msgId];
        }

        // Definately the message result is not set, so we add the host to the
        // waiters list
        SPDLOG_DEBUG("Adding host {} on the waiting list for message {}",
                     msg->mainhost(),
                     msgId);
        state.appResultWaiters[msgId].push_back(msg->mainhost());
    }

    return nullptr;
}

std::shared_ptr<faabric::BatchExecuteRequestStatus> Planner::getBatchResults(
  int32_t appId)
{
    auto berStatus = faabric::util::batchExecStatusFactory(appId);

    // Acquire a read lock to copy all the results we have for this batch
    {
        faabric::util::SharedLock lock(plannerMx);

        if (!state.appResults.contains(appId)) {
            return nullptr;
        }

        for (auto msgResultPair : state.appResults.at(appId)) {
            *berStatus->add_messageresults() = *(msgResultPair.second);
        }
    }

    return berStatus;
}

static faabric::batch_scheduler::HostMap convertToBatchSchedHostMap(
  std::map<std::string, std::shared_ptr<Host>> hostMapIn)
{
    faabric::batch_scheduler::HostMap hostMap;

    for (const auto& [ip, host] : hostMapIn) {
        hostMap[ip] = std::make_shared<faabric::batch_scheduler::HostState>(
          host->ip(), host->slots(), host->usedslots());
    }

    return hostMap;
}

std::shared_ptr<faabric::batch_scheduler::SchedulingDecision>
Planner::callBatch(std::shared_ptr<BatchExecuteRequest> req)
{
    int appId = req->appid();

    // Acquire a full lock to make the scheduling decision and update the
    // in-filght map if necessary
    faabric::util::FullLock lock(plannerMx);

    auto batchScheduler = faabric::batch_scheduler::getBatchScheduler();
    auto decisionType =
      batchScheduler->getDecisionType(state.inFlightReqs, req);

    // Make a copy of the host-map state to make sure the scheduling process
    // does not modify it
    auto hostMapCopy = convertToBatchSchedHostMap(state.hostMap);

    // For a DIST_CHANGE decision (i.e. migration) we want to try to imrpove
    // on the old decision (we don't care the one we send), so we make sure
    // we are scheduling the same messages from the old request
    if (decisionType == faabric::batch_scheduler::DecisionType::DIST_CHANGE) {
        auto oldReq = state.inFlightReqs.at(appId).first;
        req->clear_messages();
        for (const auto& msg : oldReq->messages()) {
            *req->add_messages() = msg;
        }
    }

    auto decision = batchScheduler->makeSchedulingDecision(hostMapCopy, state.inFlightReqs, req);
#ifndef NDEBUG
    // Here we make sure the state hasn't changed here (we pass const, but they
    // are const pointers)
    // NOTE: maybe it is a good idea to force us to make a copy into the right
    // hostMap type, to make sure we don't modify our state here
#endif

    // Handle failures to schedule work
    if (*decision == NOT_ENOUGH_SLOTS_DECISION) {
        SPDLOG_ERROR(
          "Not enough free slots to schedule app: {} (requested: {})",
          appId,
          req->messages_size());
        return decision;
    }

    if (*decision == DO_NOT_MIGRATE_DECISION) {
        SPDLOG_DEBUG("Decided to not migrate app: {}", appId);
        return decision;
    }

    // A scheduling decision will create a new PTP mapping and, as a
    // consequence, a new group ID
    int newGroupId = faabric::util::generateGid();
    decision->groupId = newGroupId;
    faabric::util::updateBatchExecGroupId(req, newGroupId);

    // Given a scheduling decision, depending on the decision type, we want to:
    // 1. Update the host-map to reflect the new host occupation
    // 2. Update the in-flight map to include the new request
    // 3. Send the PTP mappings to all the hosts involved
    auto& broker = faabric::transport::getPointToPointBroker();
    switch (decisionType) {
        case faabric::batch_scheduler::DecisionType::NEW: {
            // 0. Log the decision in debug mode
#ifndef NDEBUG
            decision->print();
#endif

            // 1. For a scale change request, we only need to update the hosts
            // with the new messages being scheduled
            for (int i = 0; i < decision->hosts.size(); i++) {
                // TODO: re-factor into something nicer
                state.hostMap.at(decision->hosts.at(i))
                  ->set_usedslots(
                    state.hostMap.at(decision->hosts.at(i))->usedslots() + 1);
                assert(state.hostMap.at(decision->hosts.at(i))->usedslots() <=
                       state.hostMap.at(decision->hosts.at(i))->slots());
            }

            // 2. For a new decision, we just add it to the in-flight map
            state.inFlightReqs[appId] = std::make_pair(req, decision);

            // 3. We send the mappings to all the hosts involved
            broker.setAndSendMappingsFromSchedulingDecision(*decision);

            break;
        }
        case faabric::batch_scheduler::DecisionType::SCALE_CHANGE: {
            // 1. For a scale change request, we only need to update the hosts
            // with the _new_ messages being scheduled
            for (int i = 0; i < decision->hosts.size(); i++) {
                // TODO: re-factor into something nicer
                state.hostMap.at(decision->hosts.at(i))
                  ->set_usedslots(
                    state.hostMap.at(decision->hosts.at(i))->usedslots() + 1);
                assert(state.hostMap.at(decision->hosts.at(i))->usedslots() <=
                       state.hostMap.at(decision->hosts.at(i))->slots());
            }

            // 2. For a scale change request, we want to update the BER with the
            // _new_ messages we are adding
            auto oldReq = state.inFlightReqs.at(appId).first;
            auto oldDec = state.inFlightReqs.at(appId).second;
            faabric::util::updateBatchExecGroupId(oldReq, newGroupId);
            oldDec->groupId = newGroupId;

            for (int i = 0; i < req->messages_size(); i++) {
                *oldReq->add_messages() = req->messages(i);
                oldDec->addMessage(decision->hosts.at(i), req->messages(i));
            }

            // 2.5. Log the updated decision in debug mode
#ifndef NDEBUG
            oldDec->print();
#endif

            // 3. We want to send the mappings for the _updated_ decision,
            // including _all_ the messages (not just the ones that are being
            // added)
            broker.setAndSendMappingsFromSchedulingDecision(*oldDec);

            break;
        }
        case faabric::batch_scheduler::DecisionType::DIST_CHANGE: {
            // 0. Log the decision in debug mode
#ifndef NDEBUG
            decision->print();
#endif

            auto oldReq = state.inFlightReqs.at(appId).first;
            auto oldDec = state.inFlightReqs.at(appId).second;
            // We want to let all hosts involved in the migration (not only
            // those in the new decision) that we are gonna migrate. For the
            // evicted hosts (those present in the old decision but not in the
            // new one) we need to send the mappings manually
            // TODO: can we make this cleaner?
            std::vector<std::string> evictedHostsVec;
            // TODO FIXME why do we need to use vectors and not sets?
            std::vector<std::string> oldDecHosts = oldDec->hosts;
            std::sort(oldDecHosts.begin(), oldDecHosts.end());
            std::vector<std::string> newDecHosts = decision->hosts;
            std::sort(newDecHosts.begin(), newDecHosts.end());
            /*
            std::set_difference(oldDec->uniqueHosts().cbegin(),
                                oldDec->uniqueHosts().cend(),
                                decision->uniqueHosts().cbegin(),
                                decision->uniqueHosts().cend(),
            */
            std::set_difference(oldDecHosts.begin(),
                                oldDecHosts.end(),
                                newDecHosts.begin(),
                                newDecHosts.end(),
                                std::back_inserter(evictedHostsVec));
            std::set<std::string> evictedHosts(evictedHostsVec.begin(),
                                               evictedHostsVec.end());

            // 1. We only need to update the hosts where both decisions differ
            assert(decision->hosts.size() == oldDec->hosts.size());
            for (int i = 0; i < decision->hosts.size(); i++) {
                if (decision->hosts.at(i) == oldDec->hosts.at(i)) {
                    continue;
                }
                // TODO: re-factor into something nicer
                state.hostMap.at(oldDec->hosts.at(i))
                  ->set_usedslots(
                    state.hostMap.at(oldDec->hosts.at(i))->usedslots() - 1);
                assert(state.hostMap.at(oldDec->hosts.at(i))->usedslots() >= 0);
                state.hostMap.at(decision->hosts.at(i))
                  ->set_usedslots(
                    state.hostMap.at(decision->hosts.at(i))->usedslots() + 1);
                assert(state.hostMap.at(decision->hosts.at(i))->usedslots() <=
                       state.hostMap.at(decision->hosts.at(i))->slots());
            }

            // 2. For a DIST_CHANGE request (migration), we want to replace the
            // exsiting decision with the new one
            faabric::util::updateBatchExecGroupId(oldReq, newGroupId);
            state.inFlightReqs.at(appId) = std::make_pair(oldReq, decision);

            // 3. We want to sent the new scheduling decision to all the hosts
            // involved in the migration (even the ones that are evicted)
            broker.setAndSendMappingsFromSchedulingDecision(*decision);
            broker.sendMappingsFromSchedulingDecision(*decision, evictedHosts);

            break;
        }
        default: {
            SPDLOG_ERROR("Unrecognised decision type: {} (app: {})",
                         decisionType,
                         req->appid());
            throw std::runtime_error("Unrecognised decision type");
        }
    }

    // Sanity-checks before actually dispatching functions for execution
    assert(req->messages_size() == decision->hosts.size());
    assert(req->appid() == decision->appId);
    assert(req->groupid() == decision->groupId);

    // Lastly, asynchronously dispatch the decision to the corresponding hosts
    // (we may not need the lock here anymore, but we are eager to make the
    // whole function atomic)
    dispatchSchedulingDecision(req, decision);

    return decision;
}

void Planner::dispatchSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  std::shared_ptr<faabric::batch_scheduler::SchedulingDecision> decision)
{
    std::map<std::string, std::shared_ptr<faabric::BatchExecuteRequest>>
      hostRequests;

    assert(req->messages_size() == decision->hosts.size());

    // First we build all the BatchExecuteRequests for all the different hosts.
    // We need to keep a map as the hosts may not be contiguous in the decision
    // (i.e. we may have (hostA, hostB, hostA)
    for (int i = 0; i < req->messages_size(); i++) {
        auto msg = req->messages().at(i);

        // Initialise the BER if it is not there
        std::string thisHost = decision->hosts.at(i);
        if (hostRequests.find(thisHost) == hostRequests.end()) {
            hostRequests[thisHost] = faabric::util::batchExecFactory();
            hostRequests[thisHost]->set_appid(decision->appId);
            hostRequests[thisHost]->set_groupid(decision->groupId);
            hostRequests[thisHost]->set_user(msg.user());
            hostRequests[thisHost]->set_function(msg.function());
            hostRequests[thisHost]->set_snapshotkey(req->snapshotkey());
            hostRequests[thisHost]->set_type(req->type());
            hostRequests[thisHost]->set_subtype(req->subtype());
            hostRequests[thisHost]->set_contextdata(req->contextdata());
        }

        *hostRequests[thisHost]->add_messages() = msg;
    }

    for (const auto& [hostIp, hostReq] : hostRequests) {
        SPDLOG_DEBUG("Dispatching {} messages to host {} for execution",
                     hostReq->messages_size(),
                     hostIp);
        assert(faabric::util::isBatchExecRequestValid(hostReq));
        faabric::scheduler::getFunctionCallClient(hostIp)->executeFunctions(
          hostReq);
    }

    SPDLOG_DEBUG("Finished dispatching {} messages for execution",
                 req->messages_size());
}

Planner& getPlanner()
{
    static Planner planner;
    return planner;
}
}
