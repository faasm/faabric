#include <faabric/planner/Planner.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/clock.h>
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
    config.set_hosttimeout(std::stoi(faabric::util::getEnvVar("PLANNER_HOST_KEEPALIVE_TIMEOUT", "5")));

    printConfig();
}

PlannerConfig Planner::getConfig()
{
    // TODO: the config may not change often, so we may not even need a read
    // lock here
    faabric::util::SharedLock lock(plannerMx);

    return config;
}

void Planner::printConfig() const
{
    SPDLOG_INFO("--- System ---");
    SPDLOG_INFO("KEEP_ALIVE_TIMEOUT         {}", config.hosttimeout());
}

// Deliberately take a const reference as an argument to force a copy and take
// ownership of the host
bool Planner::registerHost(const Host& hostIn, int* hostId)
{
    // Sanity check the input argument
    if (hostIn.slots() <= 0) {
        SPDLOG_ERROR("Received erroneous request to register host {} with {} slots",
                     hostIn.ip(),
                     hostIn.slots());
        return false;
    }

    faabric::util::FullLock lock(plannerMx);

    auto it = state.hostMap.find(hostIn.ip());
    if (it == state.hostMap.end()) {
        // If its the first time we see this IP, give it a UID and add it to
        // the map
        *hostId = faabric::util::generateGid();
        SPDLOG_DEBUG("Registering host {} for the first time (host id: {}",
                     hostIn.ip(),
                     *hostId);
        state.hostMap.emplace(std::make_pair<std::string, std::shared_ptr<Host>>(
                (std::string) hostIn.ip(),
                std::make_shared<Host>(hostIn)));
        state.hostMap.at(hostIn.ip())->set_hostid(*hostId);
    } else if (it->second->hostid() != hostIn.hostid()) {
        SPDLOG_ERROR("Received register request for a registered host, but"
                     "with different request ids: {} != {} (host: {})",
                     hostIn.hostid(), it->second->hostid(), hostIn.ip());
        return false;
    }

    // Overwrite the timestamp
    SPDLOG_DEBUG("Overwriting timestamp for host {} (id: {})",
                 hostIn.ip(),
                 state.hostMap.at(hostIn.ip())->hostid());
    state.hostMap.at(hostIn.ip())->mutable_registerts()->set_epochms(
      faabric::util::getGlobalClock().epochMillis());

    return true;
}

Planner& getPlanner()
{
    static Planner planner;
    return planner;
}
}
