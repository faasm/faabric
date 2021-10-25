#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#define MAPPING_TIMEOUT_MS 10000

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

std::string getPointToPointKey(int groupId, int sendIdx, int recvIdx)
{
    return fmt::format("{}-{}-{}", groupId, sendIdx, recvIdx);
}

std::string getPointToPointKey(int groupId, int recvIdx)
{
    return fmt::format("{}-{}", groupId, recvIdx);
}

PointToPointBroker::PointToPointBroker()
  : sch(faabric::scheduler::getScheduler())
{}

std::string PointToPointBroker::getHostForReceiver(int groupId, int recvIdx)
{
    faabric::util::SharedLock lock(brokerMutex);

    std::string key = getPointToPointKey(groupId, recvIdx);

    if (mappings.find(key) == mappings.end()) {
        SPDLOG_ERROR(
          "No point-to-point mapping for group {} idx {}", groupId, recvIdx);
        throw std::runtime_error("No point-to-point mapping found");
    }

    return mappings[key];
}

std::set<std::string>
PointToPointBroker::setUpLocalMappingsFromSchedulingDecision(
  const faabric::util::SchedulingDecision& decision)
{
    int groupId = decision.groupId;
    std::set<std::string> hosts;

    {
        faabric::util::FullLock lock(brokerMutex);

        // Set up the mappings
        for (int i = 0; i < decision.nFunctions; i++) {
            int recvIdx = decision.groupIdxs.at(i);
            const std::string& host = decision.hosts.at(i);

            SPDLOG_DEBUG("Setting point-to-point mapping {}:{} to {}",
                         groupId,
                         recvIdx,
                         host);

            // Record this index for this group
            groupIdIdxsMap[groupId].insert(recvIdx);

            // Add host mapping
            std::string key = getPointToPointKey(groupId, recvIdx);
            mappings[key] = host;

            // If it's not this host, add to set of returned hosts
            if (host != faabric::util::getSystemConfig().endpointHost) {
                hosts.insert(host);
            }
        }
    }

    {
        // Lock this group
        std::unique_lock<std::mutex> lock(groupMappingMutexes[groupId]);

        // Enable the group
        groupMappingsFlags[groupId] = true;

        // Notify waiters
        groupMappingCvs[groupId].notify_all();
    }

    return hosts;
}

void PointToPointBroker::setAndSendMappingsFromSchedulingDecision(
  const faabric::util::SchedulingDecision& decision)
{
    int groupId = decision.groupId;

    // Set up locally
    std::set<std::string> otherHosts =
      setUpLocalMappingsFromSchedulingDecision(decision);

    // Send out to other hosts
    for (const auto& host : otherHosts) {
        faabric::PointToPointMappings msg;
        msg.set_groupid(groupId);

        std::set<int>& indexes = groupIdIdxsMap[groupId];

        for (int i = 0; i < decision.nFunctions; i++) {
            auto* mapping = msg.add_mappings();
            mapping->set_host(decision.hosts.at(i));
            mapping->set_messageid(decision.messageIds.at(i));
            mapping->set_recvidx(decision.groupIdxs.at(i));
        }

        SPDLOG_DEBUG("Sending {} point-to-point mappings for {} to {}",
                     indexes.size(),
                     groupId,
                     host);

        auto cli = getClient(host);
        cli->sendMappings(msg);
    }
}

void PointToPointBroker::waitForMappingsOnThisHost(int groupId)
{
    // Check if it's been enabled
    if (!groupMappingsFlags[groupId]) {
        // Lock this app
        std::unique_lock<std::mutex> lock(groupMappingMutexes[groupId]);

        // Wait for app to be enabled
        auto timePoint = std::chrono::system_clock::now() +
                         std::chrono::milliseconds(MAPPING_TIMEOUT_MS);

        if (!groupMappingCvs[groupId].wait_until(
              lock, timePoint, [this, groupId] {
                  return groupMappingsFlags[groupId];
              })) {

            SPDLOG_ERROR("Timed out waiting for app mappings {}", groupId);
            throw std::runtime_error("Timed out waiting for app mappings");
        }
    }
}

std::set<int> PointToPointBroker::getIdxsRegisteredForGroup(int groupId)
{
    faabric::util::SharedLock lock(brokerMutex);
    return groupIdIdxsMap[groupId];
}

void PointToPointBroker::sendMessage(int groupid,
                                     int sendIdx,
                                     int recvIdx,
                                     const uint8_t* buffer,
                                     size_t bufferSize)
{
    waitForMappingsOnThisHost(groupid);

    std::string host = getHostForReceiver(groupid, recvIdx);

    if (host == faabric::util::getSystemConfig().endpointHost) {
        std::string label = getPointToPointKey(groupid, sendIdx, recvIdx);

        // Note - this map is thread-local so no locking required
        if (sendEndpoints.find(label) == sendEndpoints.end()) {
            sendEndpoints[label] =
              std::make_unique<AsyncInternalSendMessageEndpoint>(label);

            SPDLOG_TRACE("Created new internal send endpoint {}",
                         sendEndpoints[label]->getAddress());
        }

        SPDLOG_TRACE("Local point-to-point message {}:{}:{} to {}",
                     groupid,
                     sendIdx,
                     recvIdx,
                     sendEndpoints[label]->getAddress());

        sendEndpoints[label]->send(buffer, bufferSize);

    } else {
        auto cli = getClient(host);
        faabric::PointToPointMessage msg;
        msg.set_groupid(groupid);
        msg.set_sendidx(sendIdx);
        msg.set_recvidx(recvIdx);
        msg.set_data(buffer, bufferSize);

        SPDLOG_TRACE("Remote point-to-point message {}:{}:{} to {}",
                     groupid,
                     sendIdx,
                     recvIdx,
                     host);

        cli->sendMessage(msg);
    }
}

std::vector<uint8_t> PointToPointBroker::recvMessage(int groupId,
                                                     int sendIdx,
                                                     int recvIdx)
{
    std::string label = getPointToPointKey(groupId, sendIdx, recvIdx);

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

    groupIdIdxsMap.clear();
    mappings.clear();

    groupMappingMutexes.clear();
    groupMappingsFlags.clear();
    groupMappingCvs.clear();
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
