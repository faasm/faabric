#include "faabric/util/config.h"
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/transport/PointToPointRegistry.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::transport {

PointToPointRegistry::PointToPointRegistry()
  : sch(faabric::scheduler::getScheduler())
{}

std::string PointToPointRegistry::getKey(int appId, int recvIdx)
{
    return fmt::format("{}-{}", appId, recvIdx);
}

std::string PointToPointRegistry::getHostForReceiver(int appId, int recvIdx)
{
    faabric::util::SharedLock lock(registryMutex);

    std::string key = getKey(appId, recvIdx);

    if (mappings.find(key) == mappings.end()) {
        SPDLOG_ERROR(
          "No point-to-point mapping for app {} idx {}", appId, recvIdx);
        throw std::runtime_error("No point-to-point mapping found");
    }

    return mappings[key];
}

void PointToPointRegistry::setHostForReceiver(int appId,
                                              int recvIdx,
                                              const std::string& host)
{
    faabric::util::FullLock lock(registryMutex);

    SPDLOG_TRACE(
      "Setting point-to-point mapping {}:{} to {}", appId, recvIdx, host);

    // Record this index for this app
    appIdxs[appId].insert(recvIdx);

    // Add host mapping
    std::string key = getKey(appId, recvIdx);
    mappings[key] = host;
}

void PointToPointRegistry::broadcastMappings(int appId)
{
    auto& sch = faabric::scheduler::getScheduler();

    // TODO seems excessive to broadcast to all hosts, could we perhaps use the
    // set of registered hosts?
    std::set<std::string> hosts = sch.getAvailableHosts();

    for (const auto& host : hosts) {
        sendMappings(appId, host);
    }
}

void PointToPointRegistry::sendMappings(int appId, const std::string& host)
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

    PointToPointClient cli(host);
    cli.sendMappings(msg);
}

std::set<int> PointToPointRegistry::getIdxsRegisteredForApp(int appId)
{
    faabric::util::SharedLock lock(registryMutex);
    return appIdxs[appId];
}

void PointToPointRegistry::sendMessage(int appId,
                                       int sendIdx,
                                       int recvIdx,
                                       const uint8_t* buffer,
                                       size_t bufferSize)
{
    std::string host = getHostForReceiver(appId, recvIdx);

    // TODO - if this host, put directly onto queue
    if (host == faabric::util::getSystemConfig().endpointHost) {
        std::string label = getPointToPointInprocLabel(appId, sendIdx, recvIdx);

        // TODO - need to keep this endpoint in scope in the same thread until
        // the message is consumed.
        std::unique_ptr<AsyncInternalSendMessageEndpoint> sendEndpoint =
          std::make_unique<AsyncInternalSendMessageEndpoint>(label);
    } else {
        // TODO - if not, create client and send to remote host
    }
}

std::vector<uint8_t> PointToPointRegistry::recvMessage(int appId,
                                                       int sendIdx,
                                                       int recvIdx)
{
    std::string label = getPointToPointInprocLabel(appId, sendIdx, recvIdx);
    std::unique_ptr<AsyncInternalRecvMessageEndpoint> endpoint =
      std::make_unique<AsyncInternalRecvMessageEndpoint>(label);

    std::optional<Message> messageDataMaybe = endpoint->recv().value();
    Message messageData = messageDataMaybe.value();

    // TODO - possible to avoid this copy?
    return messageData.dataCopy();
}

void PointToPointRegistry::clear()
{
    faabric::util::SharedLock lock(registryMutex);

    appIdxs.clear();
    mappings.clear();
}

PointToPointRegistry& getPointToPointRegistry()
{
    static PointToPointRegistry reg;
    return reg;
}
}
