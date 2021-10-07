#include "faabric/util/config.h"
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::transport {

std::string getPointToPointKey(int appId, int sendIdx, int recvIdx)
{
    return fmt::format("{}-{}-{}", appId, sendIdx, recvIdx);
}

std::string getPointToPointKey(int appId, int recvIdx)
{
    return fmt::format("{}-{}", appId, recvIdx);
}

PointToPointBroker::PointToPointBroker()
  : sch(faabric::scheduler::getScheduler())
{}

std::string PointToPointBroker::getHostForReceiver(int appId, int recvIdx)
{
    faabric::util::SharedLock lock(registryMutex);

    std::string key = getPointToPointKey(appId, recvIdx);

    if (mappings.find(key) == mappings.end()) {
        SPDLOG_ERROR(
          "No point-to-point mapping for app {} idx {}", appId, recvIdx);
        throw std::runtime_error("No point-to-point mapping found");
    }

    return mappings[key];
}

void PointToPointBroker::setHostForReceiver(int appId,
                                            int recvIdx,
                                            const std::string& host)
{
    faabric::util::FullLock lock(registryMutex);

    SPDLOG_TRACE(
      "Setting point-to-point mapping {}:{} to {}", appId, recvIdx, host);

    // Record this index for this app
    appIdxs[appId].insert(recvIdx);

    // Add host mapping
    std::string key = getPointToPointKey(appId, recvIdx);
    mappings[key] = host;
}

void PointToPointBroker::broadcastMappings(int appId)
{
    auto& sch = faabric::scheduler::getScheduler();

    // TODO seems excessive to broadcast to all hosts, could we perhaps use the
    // set of registered hosts?
    std::set<std::string> hosts = sch.getAvailableHosts();

    for (const auto& host : hosts) {
        sendMappings(appId, host);
    }
}

void PointToPointBroker::sendMappings(int appId, const std::string& host)
{
    faabric::util::SharedLock lock(registryMutex);

    faabric::PointToPointMappings msg;

    std::set<int>& indexes = appIdxs[appId];

    for (auto i : indexes) {
        std::string key = getPointToPointKey(appId, i);
        std::string host = mappings[key];

        auto* mapping = msg.add_mappings();
        mapping->set_appid(appId);
        mapping->set_recvidx(i);
        mapping->set_host(host);
    }

    PointToPointClient cli(host);
    cli.sendMappings(msg);
}

std::set<int> PointToPointBroker::getIdxsRegisteredForApp(int appId)
{
    faabric::util::SharedLock lock(registryMutex);
    return appIdxs[appId];
}

void PointToPointBroker::sendMessage(int appId,
                                     int sendIdx,
                                     int recvIdx,
                                     const uint8_t* buffer,
                                     size_t bufferSize)
{
    std::string host = getHostForReceiver(appId, recvIdx);

    if (host == faabric::util::getSystemConfig().endpointHost) {
        std::string label = getPointToPointKey(appId, sendIdx, recvIdx);

        // TODO - need to keep this endpoint in scope in the same thread until
        // the message is consumed.
        AsyncInternalSendMessageEndpoint endpoint(label);
        SPDLOG_TRACE("Local point-to-point message {}:{}:{} to {}",
                     appId,
                     sendIdx,
                     recvIdx,
                     endpoint.getAddress());

        endpoint.send(buffer, bufferSize);

    } else {
        PointToPointClient cli(host);
        faabric::PointToPointMessage msg;
        msg.set_appid(appId);
        msg.set_sendidx(sendIdx);
        msg.set_recvidx(recvIdx);
        msg.set_data(buffer, bufferSize);

        SPDLOG_TRACE("Remote point-to-point message {}:{}:{} to {}",
                     appId,
                     sendIdx,
                     recvIdx,
                     host);

        cli.sendMessage(msg);
    }
}

std::vector<uint8_t> PointToPointBroker::recvMessage(int appId,
                                                     int sendIdx,
                                                     int recvIdx)
{
    std::string label = getPointToPointKey(appId, sendIdx, recvIdx);
    AsyncInternalRecvMessageEndpoint endpoint(label);

    std::optional<Message> messageDataMaybe = endpoint.recv().value();
    Message messageData = messageDataMaybe.value();

    // TODO - possible to avoid this copy?
    return messageData.dataCopy();
}

void PointToPointBroker::clear()
{
    faabric::util::SharedLock lock(registryMutex);

    appIdxs.clear();
    mappings.clear();
}

PointToPointBroker& getPointToPointBroker()
{
    static PointToPointBroker reg;
    return reg;
}
}
