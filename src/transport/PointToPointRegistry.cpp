#include "faabric/transport/PointToPointClient.h"
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointRegistry.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::transport {

PointToPointRegistry::PointToPointRegistry() {}

std::string PointToPointRegistry::getKey(int appId, int recvIdx)
{
    return fmt::format("{}-{}", appId, recvIdx);
}

std::string PointToPointRegistry::getHostForReceiver(int appId, int recvIdx)
{
    faabric::util::SharedLock lock(registryMutex);

    std::string key = getKey(appId, recvIdx);

    if (mappings.find(key) == mappings.end()) {
        SPDLOG_ERROR("No point-to-point mapping for app {} idx {}", appId, recvIdx);
        throw std::runtime_error("No point-to-point mapping found");
    }

    return mappings[key];
}

void PointToPointRegistry::setHostForReceiver(int appId,
                                              int recvIdx,
                                              const std::string& host)
{
    faabric::util::FullLock lock(registryMutex);

    // Record this index for this app
    appIdxs[appId].insert(recvIdx);

    // Add host mapping
    std::string key = getKey(appId, recvIdx);
    mappings[key] = host;
}

void PointToPointRegistry::broadcastMappings(int appId)
{
    faabric::util::SharedLock lock(registryMutex);

    faabric::PointToPointMappings msg;

    std::set<int>& indexes = appIdxs[appId];

    for (auto i : indexes) {
        std::string key = getKey(appId, i);
        std::string host = mappings[key];

        auto* mapping = msg.add_mappings();
        mapping->set_appid(appId);
        mapping->set_recvidx(i);
        mapping->set_host(host);
    }

    auto& sch = faabric::scheduler::getScheduler();

    // TODO seems excessive to broadcast to all hosts, could we perhaps use the
    // set of registered hosts?
    std::set<std::string> hosts = sch.getAvailableHosts();

    for (const auto& host : hosts) {
        PointToPointClient cli(host);
        cli.sendMappings(msg);
    }
}

std::set<int> PointToPointRegistry::getIdxsRegisteredForApp(int appId)
{
    faabric::util::SharedLock lock(registryMutex);
    return appIdxs[appId];
}

void PointToPointRegistry::clear()
{
    faabric::util::SharedLock lock(registryMutex);

    mappings.clear();
}

PointToPointRegistry& getPointToPointRegistry()
{
    static PointToPointRegistry reg;
    return reg;
}
}
