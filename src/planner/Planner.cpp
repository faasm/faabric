#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/planner/Planner.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/common.h>
#include <faabric/util/batch.h>
#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <string>

// Special group ID magic to indicate MPI decisions that we have preemptively
// scheduled
#define MPI_PRELOADED_DECISION_GROUPID -99

namespace faabric::planner {

// ----------------------
// Utility Functions
// ----------------------

static void claimHostSlots(std::shared_ptr<Host> host, int slotsToClaim = 1)
{
    host->set_usedslots(host->usedslots() + slotsToClaim);
    assert(host->usedslots() <= host->slots());
}

static void releaseHostSlots(std::shared_ptr<Host> host, int slotsToRelease = 1)
{
    host->set_usedslots(host->usedslots() - slotsToRelease);
    assert(host->usedslots() >= 0);
}

static int claimHostMpiPort(std::shared_ptr<Host> host)
{
    for (int i = 0; i < host->mpiports_size(); i++) {
        if (!host->mpiports(i).used()) {
            host->mutable_mpiports()->at(i).set_used(true);
            SPDLOG_DEBUG("Assigning MPI port {} for host {}",
                         host->mpiports(i).port(),
                         host->ip());
            return host->mpiports(i).port();
        }
    }

    SPDLOG_ERROR("Ran out of MPI ports trying to claim one!");
    throw std::runtime_error("Ran out of MPI ports!");
}

static void releaseHostMpiPort(std::shared_ptr<Host> host, int mpiPort)
{
    SPDLOG_DEBUG("Releasing port {} for host {}", mpiPort, host->ip());

    for (int i = 0; i < host->mpiports_size(); i++) {
        if (host->mpiports(i).port() == mpiPort) {
            assert(host->mpiports(i).used());
            host->mutable_mpiports()->at(i).set_used(false);
            return;
        }
    }

    SPDLOG_ERROR("Requested to free unavailbale MPI port: {}", mpiPort);
    throw std::runtime_error("Requested to free unavailable MPI port!");
}

static void printHostState(std::map<std::string, std::shared_ptr<Host>> hostMap,
                           const std::string& logLevel = "debug")
{
    std::string printedText;
    std::string header = "\n-------------- Host Map --------------";
    std::string subhead = "Ip\t\tSlots";
    std::string footer = "--------------------------------------";

    printedText += header + "\n" + subhead + "\n";
    for (const auto& [ip, hostState] : hostMap) {
        printedText += fmt::format(
          "{}\t{}/{}\n", ip, hostState->usedslots(), hostState->slots());
    }
    printedText += footer;

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

// ----------------------
// Planner
// ----------------------

// Planner is used globally as a static variable. This constructor relies on
// the fact that C++ static variable's initialisation is thread-safe
Planner::Planner()
  : snapshotRegistry(faabric::snapshot::getSnapshotRegistry())
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

    flushSchedulingState();

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
        case faabric::planner::FlushType::SchedulingState:
            SPDLOG_INFO("Planner flushing scheduling state");
            flushSchedulingState();
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

void Planner::flushSchedulingState()
{
    faabric::util::FullLock lock(plannerMx);

    state.inFlightReqs.clear();
    state.appResults.clear();
    state.appResultWaiters.clear();
    state.numMigrations = 0;
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

        // Populate the mpi ports
        for (int i = 0; i < hostIn.slots(); i++) {
            auto* mpiPort = state.hostMap.at(hostIn.ip())->add_mpiports();
            mpiPort->set_port(MPI_BASE_PORT + i);
            mpiPort->set_used(false);
        }
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

        it->second->clear_mpiports();
        for (int i = 0; i < hostIn.slots(); i++) {
            auto* mpiPort = state.hostMap.at(hostIn.ip())->add_mpiports();
            mpiPort->set_port(MPI_BASE_PORT + i);
            // If we are overwritting with a number of used slots, we
            // set the first ports as used too
            mpiPort->set_used(i < hostIn.usedslots());
        }
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
    SPDLOG_INFO("Planner received request to remove host {}", hostIn.ip());

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

    // If we are dealing with a migrated message, we can ignore the setting
    // of the message result. This is because the migrated function will
    // have the same message id, and thus we will call this method again with
    // a message with the same message id. In addition, all the in-flihght
    // state is updated accordingly when we schedule the migration
    bool isMigratedMsg = msg->returnvalue() == MIGRATED_FUNCTION_RETURN_VALUE;
    if (isMigratedMsg) {
        return;
    }

    faabric::util::FullLock lock(plannerMx);

    SPDLOG_DEBUG("Planner setting message result (id: {}) for {}:{}:{}",
                 msg->id(),
                 msg->appid(),
                 msg->groupid(),
                 msg->groupidx());

    // Release the slot only once
    assert(state.hostMap.contains(msg->executedhost()));
    if (!state.appResults[appId].contains(msgId)) {
        releaseHostSlots(state.hostMap.at(msg->executedhost()));
    }

    // Set the result
    state.appResults[appId][msgId] = msg;

    // Remove the message from the in-flight requests
    if (!state.inFlightReqs.contains(appId)) {
        // We don't want to error if any client uses `setMessageResult`
        // liberally. This means that it may happen that when we set a message
        // result a second time, the app is already not in-flight
        SPDLOG_DEBUG("Setting result for non-existant (or finished) app: {}",
                     appId);
    } else {
        auto req = state.inFlightReqs.at(appId).first;
        auto decision = state.inFlightReqs.at(appId).second;

        // Work out the message position in the BER
        auto it = std::find_if(
          req->messages().begin(), req->messages().end(), [&](auto innerMsg) {
              return innerMsg.id() == msg->id();
          });
        if (it == req->messages().end()) {
            // Ditto as before. We want to allow setting the message result
            // more than once without breaking
            SPDLOG_DEBUG(
              "Setting result for non-existant (or finished) message: {}",
              appId);
        } else {
            SPDLOG_DEBUG("Removing message {} from app {}", msg->id(), appId);

            // Remove message from in-flight requests
            req->mutable_messages()->erase(it);

            // Remove message from decision
            int freedMpiPort = decision->removeMessage(msg->id());
            releaseHostMpiPort(state.hostMap.at(msg->executedhost()),
                               freedMpiPort);

            // Remove pair altogether if no more messages left
            if (req->messages_size() == 0) {
                SPDLOG_INFO("Planner removing app {} from in-flight", appId);
                assert(decision->nFunctions == 0);
                assert(decision->hosts.empty());
                assert(decision->messageIds.empty());
                assert(decision->appIdxs.empty());
                assert(decision->groupIdxs.empty());
                assert(decision->mpiPorts.empty());
                state.inFlightReqs.erase(appId);

                // If we are removing the app from in-flight, we can also
                // remmove any pre-loaded scheduling decisions
                if (state.preloadedSchedulingDecisions.contains(appId)) {
                    SPDLOG_DEBUG(
                      "Removing preloaded scheduling decision for app {}",
                      appId);
                    state.preloadedSchedulingDecisions.erase(appId);
                }
            }
        }
    }

    // Finally, dispatch an async message to all hosts that are waiting once
    // all planner accounting is updated
    if (state.appResultWaiters.find(msgId) != state.appResultWaiters.end()) {
        for (const auto& host : state.appResultWaiters[msgId]) {
            SPDLOG_DEBUG("Sending result to waiting host: {}", host);
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

void Planner::preloadSchedulingDecision(
  int32_t appId,
  std::shared_ptr<batch_scheduler::SchedulingDecision> decision)
{
    faabric::util::FullLock lock(plannerMx);

    if (state.preloadedSchedulingDecisions.contains(appId)) {
        SPDLOG_ERROR(
          "ERROR: preloaded scheduling decisions already contain app {}",
          appId);
        return;
    }

    SPDLOG_INFO("Pre-loading scheduling decision for app {}", appId);
    state.preloadedSchedulingDecisions[appId] = decision;
}

std::shared_ptr<batch_scheduler::SchedulingDecision>
Planner::getPreloadedSchedulingDecision(
  int32_t appId,
  std::shared_ptr<BatchExecuteRequest> ber)
{
    SPDLOG_DEBUG("Requesting pre-loaded scheduling decision for app {}", appId);
    // WARNING: this method is currently only called from the main Planner
    // entrypoint (callBatch) which has a FullLock, thus we don't need to
    // acquire a (SharedLock) here. In general, we would need a read-lock
    // to read the dict from the planner's state
    auto decision = state.preloadedSchedulingDecisions.at(appId);
    assert(decision != nullptr);

    // Only include in the returned scheduling decision the group indexes that
    // are in this BER. This can happen when consuming a preloaded decision
    // in two steps (e.g. for MPI)
    std::shared_ptr<batch_scheduler::SchedulingDecision> filteredDecision =
      std::make_shared<batch_scheduler::SchedulingDecision>(decision->appId,
                                                            decision->groupId);
    for (const auto& msg : ber->messages()) {
        int groupIdx = msg.groupidx();
        int idxInDecision = std::distance(decision->groupIdxs.begin(),
                                          std::find(decision->groupIdxs.begin(),
                                                    decision->groupIdxs.end(),
                                                    groupIdx));
        assert(idxInDecision < decision->groupIdxs.size());

        // Add the schedulign for this group idx to the filtered decision.
        // Make sure we also maintain the message IDs that come from the BER
        // (as we can not possibly predict them in the preloaded decision)
        filteredDecision->addMessage(decision->hosts.at(idxInDecision),
                                     msg.id(),
                                     decision->appIdxs.at(idxInDecision),
                                     decision->groupIdxs.at(idxInDecision));
        filteredDecision->mpiPorts.at(filteredDecision->nFunctions - 1) =
          decision->mpiPorts.at(idxInDecision);
    }
    assert(filteredDecision->hosts.size() == ber->messages_size());

    return filteredDecision;
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

        // Set the finished condition
        berStatus->set_finished(!state.inFlightReqs.contains(appId));
    }

    return berStatus;
}

std::shared_ptr<faabric::batch_scheduler::SchedulingDecision>
Planner::getSchedulingDecision(std::shared_ptr<BatchExecuteRequest> req)
{
    int appId = req->appid();

    // Acquire a read lock to get the scheduling decision for the requested app
    faabric::util::SharedLock lock(plannerMx);

    if (state.inFlightReqs.find(appId) == state.inFlightReqs.end()) {
        return nullptr;
    }

    return state.inFlightReqs.at(appId).second;
}

faabric::batch_scheduler::InFlightReqs Planner::getInFlightReqs()
{
    faabric::util::SharedLock lock(plannerMx);

    // Deliberately deep copy here
    faabric::batch_scheduler::InFlightReqs inFlightReqsCopy;
    for (const auto& [appId, inFlightPair] : state.inFlightReqs) {
        inFlightReqsCopy[appId] = std::make_pair(
          std::make_shared<BatchExecuteRequest>(*inFlightPair.first),
          std::make_shared<faabric::batch_scheduler::SchedulingDecision>(
            *inFlightPair.second));
    }

    return inFlightReqsCopy;
}

int Planner::getNumMigrations()
{
    return state.numMigrations.load(std::memory_order_acquire);
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
    bool isDistChange =
      decisionType == faabric::batch_scheduler::DecisionType::DIST_CHANGE;

    // For a DIST_CHANGE decision (i.e. migration) we want to try to imrpove
    // on the old decision (we don't care the one we send), so we make sure
    // we are scheduling the same messages from the old request
    if (isDistChange) {
        SPDLOG_INFO("App {} asked for migration opportunities", appId);
        auto oldReq = state.inFlightReqs.at(appId).first;
        req->clear_messages();
        for (const auto& msg : oldReq->messages()) {
            *req->add_messages() = msg;
        }
    }

    // For a NEW decision of an MPI application, we know that it will be
    // followed-up by a SCALE_CHANGE one, and that the mpi_world_size parameter
    // must be set. Thus, we can schedule slots for all the MPI ranks, and
    // consume them later as a preloaded scheduling decision
    bool isNew = decisionType == faabric::batch_scheduler::DecisionType::NEW;
    bool isMpi = req->messages(0).ismpi();
    std::shared_ptr<BatchExecuteRequest> mpiReq = nullptr;

    // Check if there exists a pre-loaded scheduling decision for this app
    // (e.g. if we want to force a migration). Note that we don't want to check
    // pre-loaded decisions for dist-change requests
    std::shared_ptr<batch_scheduler::SchedulingDecision> decision = nullptr;
    if (!isDistChange && state.preloadedSchedulingDecisions.contains(appId)) {
        decision = getPreloadedSchedulingDecision(appId, req);
    } else if (isNew && isMpi) {
        mpiReq = faabric::util::batchExecFactory(
          req->user(), req->function(), req->messages(0).mpiworldsize());

        // Populate the temporary request
        mpiReq->mutable_messages()->at(0) = req->messages(0);
        faabric::util::updateBatchExecAppId(mpiReq, appId);
        for (int i = 0; i < mpiReq->messages_size(); i++) {
            mpiReq->mutable_messages()->at(i).set_groupidx(i);
        }

        decision = batchScheduler->makeSchedulingDecision(
          hostMapCopy, state.inFlightReqs, mpiReq);
    } else {
        decision = batchScheduler->makeSchedulingDecision(
          hostMapCopy, state.inFlightReqs, req);
    }
    assert(decision != nullptr);

    // Handle failures to schedule work
    if (*decision == NOT_ENOUGH_SLOTS_DECISION) {
        SPDLOG_ERROR(
          "Not enough free slots to schedule app: {} (requested: {})",
          appId,
          req->messages_size());
        printHostState(state.hostMap, "error");
        return decision;
    }

    if (*decision == DO_NOT_MIGRATE_DECISION) {
        SPDLOG_INFO("Decided to not migrate app: {}", appId);
        return decision;
    }

    // Skip claiming slots and ports if we have preemptively allocated them
    bool skipClaim = decision->groupId == MPI_PRELOADED_DECISION_GROUPID;

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
            // 1. For a new request, we only need to update the hosts
            // with the new messages being scheduled
            for (int i = 0; i < decision->hosts.size(); i++) {
                auto thisHost = state.hostMap.at(decision->hosts.at(i));
                claimHostSlots(thisHost);
                decision->mpiPorts.at(i) = claimHostMpiPort(thisHost);
            }
            assert(decision->hosts.size() == decision->mpiPorts.size());

            // 1.5. Log the decision after we have populated the MPI ports
#ifndef NDEBUG
            decision->print();
#endif

            // For a NEW MPI decision that was not preloaded we have
            // preemptively scheduled all MPI messages but now we just need to
            // return the first one, and preload the rest
            if (isMpi && mpiReq != nullptr) {
                auto mpiDecision = std::make_shared<
                  faabric::batch_scheduler::SchedulingDecision>(req->appid(),
                                                                req->groupid());
                *mpiDecision = *decision;
                mpiDecision->groupId = MPI_PRELOADED_DECISION_GROUPID;
                state.preloadedSchedulingDecisions[appId] = mpiDecision;

                // Remove all messages that we do not have to dispatch now
                for (int i = 1; i < mpiDecision->messageIds.size(); i++) {
                    decision->removeMessage(mpiDecision->messageIds.at(i));
                }
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
                auto thisHost = state.hostMap.at(decision->hosts.at(i));
                if (!skipClaim) {
                    claimHostSlots(thisHost);
                }
            }

            // 2. For a scale change request, we want to update the BER with the
            // _new_ messages we are adding
            auto oldReq = state.inFlightReqs.at(appId).first;
            auto oldDec = state.inFlightReqs.at(appId).second;
            faabric::util::updateBatchExecGroupId(oldReq, newGroupId);
            oldDec->groupId = newGroupId;

            // Update the MPI ports in the old decision
            for (int i = 0; i < req->messages_size(); i++) {
                *oldReq->add_messages() = req->messages(i);
                oldDec->addMessage(decision->hosts.at(i), req->messages(i));
                if (!skipClaim) {
                    oldDec->mpiPorts.at(oldDec->nFunctions - 1) =
                      claimHostMpiPort(state.hostMap.at(decision->hosts.at(i)));
                } else {
                    assert(decision->mpiPorts.at(i) != 0);
                    oldDec->mpiPorts.at(oldDec->nFunctions - 1) =
                      decision->mpiPorts.at(i);
                }
            }

            // 2.5.1. Log the updated decision in debug mode
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
            auto oldReq = state.inFlightReqs.at(appId).first;
            auto oldDec = state.inFlightReqs.at(appId).second;

            // We want to let all hosts involved in the migration (not only
            // those in the new decision) that we are gonna migrate. For the
            // evicted hosts (those present in the old decision but not in the
            // new one) we need to send the mappings manually

            // Work out the evicted host set (unfortunately, couldn't come up
            // with a less verbose way to do it)
            std::vector<std::string> evictedHostsVec;
            std::vector<std::string> oldDecHosts = oldDec->hosts;
            std::sort(oldDecHosts.begin(), oldDecHosts.end());
            std::vector<std::string> newDecHosts = decision->hosts;
            std::sort(newDecHosts.begin(), newDecHosts.end());
            std::set_difference(oldDecHosts.begin(),
                                oldDecHosts.end(),
                                newDecHosts.begin(),
                                newDecHosts.end(),
                                std::back_inserter(evictedHostsVec));
            std::set<std::string> evictedHosts(evictedHostsVec.begin(),
                                               evictedHostsVec.end());

            // For the moment we print the old and new decisions when
            // migrating. We print them separately to help catching bugs
            // with the accounting when transfering slots and ports
            SPDLOG_INFO("Decided to migrate app {}!", appId);
            SPDLOG_INFO("Old decision:");
            oldDec->print("info");

            // 1. Clear the slots/ports in the old decision, and then claim
            // them in the new one. Note that it needs to happen in this order
            // as we may need all claimed slots in the old decision for the
            // new one
            assert(decision->hosts.size() == oldDec->hosts.size());

            // First release the migrated-from hosts and slots
            for (int i = 0; i < oldDec->hosts.size(); i++) {
                if (decision->hosts.at(i) != oldDec->hosts.at(i)) {
                    auto oldHost = state.hostMap.at(oldDec->hosts.at(i));

                    releaseHostSlots(oldHost);
                    releaseHostMpiPort(oldHost, oldDec->mpiPorts.at(i));
                }
            }

            // Second, occupy the migrated-to slots and ports
            for (int i = 0; i < decision->hosts.size(); i++) {
                if (decision->hosts.at(i) != oldDec->hosts.at(i)) {
                    auto newHost = state.hostMap.at(decision->hosts.at(i));

                    claimHostSlots(newHost);
                    decision->mpiPorts.at(i) = claimHostMpiPort(newHost);
                }
            }

            // Print the new decision after accounting has been updated
            SPDLOG_INFO("New decision:");
            decision->print("info");
            state.numMigrations += 1;

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

    // Lastly, asynchronously dispatch the execute requests to the
    // corresponding hosts if new functions need to be spawned (not if
    // migrating)
    // We may not need the lock here anymore, but we are eager to make the
    // whole function atomic)
    if (decisionType != faabric::batch_scheduler::DecisionType::DIST_CHANGE) {
        dispatchSchedulingDecision(req, decision);
    }

    return decision;
}

void Planner::dispatchSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  std::shared_ptr<faabric::batch_scheduler::SchedulingDecision> decision)
{
    std::map<std::string, std::shared_ptr<faabric::BatchExecuteRequest>>
      hostRequests;

    assert(req->messages_size() == decision->hosts.size());
    bool isSingleHost = decision->isSingleHost();

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
            hostRequests[thisHost]->set_singlehost(isSingleHost);
            // Propagate the single host hint
            hostRequests[thisHost]->set_singlehosthint(req->singlehosthint());
        }

        *hostRequests[thisHost]->add_messages() = msg;
    }

    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;
    if (!isSingleHost && req->singlehosthint()) {
        SPDLOG_ERROR(
          "User provided single-host hint in BER, but decision is not!");
    }

    for (const auto& [hostIp, hostReq] : hostRequests) {
        SPDLOG_DEBUG("Dispatching {} messages to host {} for execution",
                     hostReq->messages_size(),
                     hostIp);
        assert(faabric::util::isBatchExecRequestValid(hostReq));

        // In a THREADS request, before sending an execution request we need to
        // push the main (caller) thread snapshot to all non-main hosts
        // FIXME: ideally, we would do this from the caller thread, once we
        // know the scheduling decision and all other threads would be awaiting
        // for the snapshot
        if (isThreads && !isSingleHost) {
            auto snapshotKey =
              faabric::util::getMainThreadSnapshotKey(hostReq->messages(0));
            try {
                auto snap = snapshotRegistry.getSnapshot(snapshotKey);

                // TODO(thread-opt): push only diffs
                if (hostIp != req->messages(0).mainhost()) {
                    faabric::snapshot::getSnapshotClient(hostIp)->pushSnapshot(
                      snapshotKey, snap);
                }
            } catch (std::runtime_error& e) {
                // Catch errors, but don't let them crash the planner. Let the
                // worker crash instead
                SPDLOG_ERROR("Snapshot {} not regsitered in planner!",
                             snapshotKey);
            }
        }

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
