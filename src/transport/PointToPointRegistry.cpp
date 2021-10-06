#include "faabric/transport/PointToPointClient.h"
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointRegistry.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::transport {

PointToPointRegistry::PointToPointRegistry() {}

std::string PointToPointRegistry::getHostForReceiver(int appId, int recvIdx)
{
    faabric::util::SharedLock lock(registryMutex);

    std::string key = getKey(appId, recvIdx);

    if (mappings.find(key) == mappings.end()) {
        SPDLOG_ERROR("No point-to-point mapping for {}/{}", appId, recvIdx);
        throw std::runtime_error("No point-to-point mapping found");
    }

    return key;
}

void PointToPointRegistry::setHostForReceiver(int appId,
                                              int recvIdx,
                                              const std::string& host)
{
    faabric::util::FullLock lock(registryMutex);

    std::string key = getKey(appId, recvIdx);

    mappings[key] = host;
}

void PointToPointRegistry::broadcastMappings(int appId,
                                             std::vector<int> indexes)
{
    faabric::util::SharedLock lock(registryMutex);

    faabric::PointToPointMappings msg;

    for (auto i : indexes) {
        std::string key = getKey(appId, i);
        std::string host = mappings[key];

        auto* mapping = msg.add_mappings();
        mapping->set_appid(appId);
        mapping->set_recvidx(i);
        mapping->set_host(host);
    }

    auto& sch = faabric::scheduler::getScheduler();

    // TODO - can we just do the set of registered hosts here somehow?
    std::set<std::string> hosts = sch.getAvailableHosts();

    for (const auto& host : hosts) {
        PointToPointClient cli(host);
        cli.sendMappings(msg);
    }
}

PointToPointRegistry& getPointToPointRegistry()
{
    static PointToPointRegistry reg;
    return reg;
}
}
