#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::transport {

// NOTE: Keeping 0MQ sockets in TLS is usually a bad idea, as they _must_ be
// closed before the global context. However, in this case it's worth it
// to cache the sockets across messages, as otherwise we'd be creating and
// destroying a lot of them under high throughput. To ensure things are cleared
// up, see the thread-local tidy-up message on this class and its usage in the
// rest of the codebase.
thread_local std::
  unordered_map<std::string, std::unique_ptr<AsyncInternalRecvMessageEndpoint>>
    recvEndpoints;

thread_local std::
  unordered_map<std::string, std::unique_ptr<AsyncInternalSendMessageEndpoint>>
    sendEndpoints;

thread_local std::unordered_map<std::string,
                                std::shared_ptr<PointToPointClient>>
  clients;

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
    faabric::util::SharedLock lock(brokerMutex);

    std::string key = getPointToPointKey(appId, recvIdx);

    if (mappings.find(key) == mappings.end()) {
        SPDLOG_ERROR(
          "No point-to-point mapping for app {} idx {}", appId, recvIdx);
        throw std::runtime_error("No point-to-point mapping found");
    }

    return mappings[key];
}

std::set<std::string>
PointToPointBroker::setUpLocalMappingsFromSchedulingDecision(
  const faabric::util::SchedulingDecision& decision)
{
    faabric::util::FullLock lock(brokerMutex);

    int appId = decision.appId;
    // Set up the mappings
    std::set<std::string> hosts;
    for (int i = 0; i < decision.nFunctions; i++) {
        int recvIdx = decision.appIdxs.at(i);
        const std::string& host = decision.hosts.at(i);

        SPDLOG_TRACE(
          "Setting point-to-point mapping {}:{} to {}", appId, recvIdx, host);

        // Record this index for this app
        appIdxs[appId].insert(recvIdx);

        // Add host mapping
        std::string key = getPointToPointKey(appId, recvIdx);
        mappings[key] = host;

        // If it's not this host, add to set of returned hosts
        if (host != faabric::util::getSystemConfig().endpointHost) {
            hosts.insert(host);
        }
    }

    // Enable the app locally
    enableApp(appId);

    return hosts;
}

void PointToPointBroker::setAndSendMappingsFromSchedulingDecision(
  const faabric::util::SchedulingDecision& decision)
{
    int appId = decision.appId;

    // Set up locally
    std::set<std::string> otherHosts =
      setUpLocalMappingsFromSchedulingDecision(decision);

    // Send out to other hosts
    for (const auto& host : otherHosts) {
        faabric::PointToPointMappings msg;
        msg.set_appid(appId);

        std::set<int>& indexes = appIdxs[appId];

        for (int i = 0; i < decision.nFunctions; i++) {
            auto* mapping = msg.add_mappings();
            mapping->set_host(decision.hosts.at(i));
            mapping->set_messageid(decision.messageIds.at(i));
            mapping->set_recvidx(decision.appIdxs.at(i));
        }

        SPDLOG_DEBUG("Sending {} point-to-point mappings for {} to {}",
                     indexes.size(),
                     appId,
                     host);

        auto cli = getClient(host);
        cli->sendMappings(msg);
    }
}

void PointToPointBroker::enableApp(int appId)
{
    std::vector<uint8_t> kickOffData = { 0 };
    std::set<int> idxs = appIdxs[appId];

    // Send a kick-off message to all indexes registered locally
    for (int idx : idxs) {
        std::string key = getPointToPointKey(appId, idx);
        if (mappings[key] == faabric::util::getSystemConfig().endpointHost) {
            sendMessage(appId, 0, idx, kickOffData.data(), kickOffData.size());
        }
    }
}

void PointToPointBroker::waitForAppToBeEnabled(int appId, int recvIdx)
{
    // Wait for the kick-off message for this index
    recvMessage(appId, 0, recvIdx);
}

std::set<int> PointToPointBroker::getIdxsRegisteredForApp(int appId)
{
    faabric::util::SharedLock lock(brokerMutex);
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

        // Note - this map is thread-local so no locking required
        if (sendEndpoints.find(label) == sendEndpoints.end()) {
            sendEndpoints[label] =
              std::make_unique<AsyncInternalSendMessageEndpoint>(label);

            SPDLOG_TRACE("Created new internal send endpoint {}",
                         sendEndpoints[label]->getAddress());
        }

        SPDLOG_TRACE("Local point-to-point message {}:{}:{} to {}",
                     appId,
                     sendIdx,
                     recvIdx,
                     sendEndpoints[label]->getAddress());

        sendEndpoints[label]->send(buffer, bufferSize);

    } else {
        auto cli = getClient(host);
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

        cli->sendMessage(msg);
    }
}

std::vector<uint8_t> PointToPointBroker::recvMessage(int appId,
                                                     int sendIdx,
                                                     int recvIdx)
{
    std::string label = getPointToPointKey(appId, sendIdx, recvIdx);

    // Note: this map is thread-local so no locking required
    if (recvEndpoints.find(label) == recvEndpoints.end()) {
        recvEndpoints[label] =
          std::make_unique<AsyncInternalRecvMessageEndpoint>(label);
        SPDLOG_TRACE("Created new internal recv endpoint {}",
                     recvEndpoints[label]->getAddress());
    }

    std::optional<Message> messageDataMaybe =
      recvEndpoints[label]->recv().value();
    Message messageData = messageDataMaybe.value();

    // TODO - possible to avoid this copy?
    return messageData.dataCopy();
}

std::shared_ptr<PointToPointClient> PointToPointBroker::getClient(
  const std::string& host)
{
    // Note - this map is thread-local so no locking required
    if (clients.find(host) == clients.end()) {
        clients[host] = std::make_shared<PointToPointClient>(host);

        SPDLOG_TRACE("Created new point-to-point client {}", host);
    }

    return clients[host];
}

void PointToPointBroker::clear()
{
    faabric::util::SharedLock lock(brokerMutex);

    appIdxs.clear();
    mappings.clear();
}

void PointToPointBroker::resetThreadLocalCache()
{
    SPDLOG_TRACE("Resetting point-to-point thread-local cache");

    sendEndpoints.clear();
    recvEndpoints.clear();
    clients.clear();
}

PointToPointBroker& getPointToPointBroker()
{
    static PointToPointBroker reg;
    return reg;
}
}
