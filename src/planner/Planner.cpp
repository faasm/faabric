#include <faabric/planner/Planner.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
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

    auto& sch = faabric::scheduler::getScheduler();
    for (const auto& host : availableHosts) {
        SPDLOG_INFO("Planner sending EXECUTOR flush to {}", host->ip());
        sch.getFunctionCallClient(host->ip())->sendFlush();
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
        SPDLOG_DEBUG("NOT overwritting host {} with {} slots (used {})",
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
    auto& sch = faabric::scheduler::getScheduler();
    if (state.appResultWaiters.find(msgId) != state.appResultWaiters.end()) {
        for (const auto& host : state.appResultWaiters[msgId]) {
            SPDLOG_INFO("Sending result to waiting host: {}", host);
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
    // provided a masterhost. The masterhost is set when dispatching a message
    // within faabric, but not when sending an HTTP request
    if (!msg->masterhost().empty()) {
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
                     msg->masterhost(),
                     msgId);
        state.appResultWaiters[msgId].push_back(msg->masterhost());
    }

    return nullptr;
}

std::shared_ptr<faabric::BatchExecuteRequestStatus> Planner::getBatchResults(int32_t appId)
{
    auto berStatus = faabric::util::batchExecStatusFactory(appId);

    // Acquire a read lock to copy all the results we have for this batch
    {
        faabric::util::SharedLock lock(plannerMx);

        if (!state.appResults.contains(appId)) { return nullptr; }

        for (auto msgResultPair : state.appResults.at(appId)) {
            *berStatus->add_messageresults() = *(msgResultPair.second);
        }
    }

    return berStatus;
}

Planner& getPlanner()
{
    static Planner planner;
    return planner;
}
}
