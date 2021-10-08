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

void PointToPointBroker::setHostForReceiver(int appId,
                                            int recvIdx,
                                            const std::string& host)
{
    faabric::util::FullLock lock(brokerMutex);

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

    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();

    for (const auto& host : hosts) {
        // Skip this host
        if (host == conf.endpointHost) {
            continue;
        }

        sendMappings(appId, host);
    }
}

void PointToPointBroker::sendMappings(int appId, const std::string& host)
{
    faabric::util::SharedLock lock(brokerMutex);

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

    SPDLOG_DEBUG("Sending {} point-to-point mappings for {} to {}",
                 indexes.size(),
                 appId,
                 host);
    PointToPointClient cli(host);
    cli.sendMappings(msg);
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

void PointToPointBroker::clear()
{
    faabric::util::SharedLock lock(brokerMutex);

    appIdxs.clear();
    mappings.clear();
}

void PointToPointBroker::resetThreadLocalCache()
{
    auto tid = (pid_t)syscall(SYS_gettid);
    SPDLOG_TRACE("Resetting point-to-point thread-local cache for thread {}",
                 tid);

    sendEndpoints.clear();
    recvEndpoints.clear();
}

PointToPointBroker& getPointToPointBroker()
{
    static PointToPointBroker reg;
    return reg;
}
}
