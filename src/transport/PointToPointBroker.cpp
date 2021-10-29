#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#define NO_LOCK_OWNER_IDX -1

#define LOCK_TIMEOUT(mx, ms)                                                   \
    auto timePoint =                                                           \
      std::chrono::system_clock::now() + std::chrono::milliseconds(ms);        \
    bool success = mx.try_lock_until(timePoint);                               \
    if (!success) {                                                            \
        throw std::runtime_error("Distributed coordination timeout");          \
    }

#define MAPPING_TIMEOUT_MS 20000

namespace faabric::transport {

static std::unordered_map<int, std::shared_ptr<PointToPointGroup>> groups;

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

static std::shared_ptr<PointToPointClient> getClient(const std::string& host)
{
    // Note - this map is thread-local so no locking required
    if (clients.find(host) == clients.end()) {
        clients[host] = std::make_shared<PointToPointClient>(host);

        SPDLOG_TRACE("Created new point-to-point client {}", host);
    }

    return clients.at(host);
}

std::string getPointToPointKey(int groupId, int sendIdx, int recvIdx)
{
    return fmt::format("{}-{}-{}", groupId, sendIdx, recvIdx);
}

std::string getPointToPointKey(int groupId, int recvIdx)
{
    return fmt::format("{}-{}", groupId, recvIdx);
}

std::shared_ptr<PointToPointGroup> PointToPointGroup::getGroup(int groupId)
{
    if (groups.find(groupId) == groups.end()) {
        SPDLOG_ERROR("Did not find group ID {} on this host", groupId);
        throw std::runtime_error("Group ID not found on host");
    }

    return groups.at(groupId);
}

bool PointToPointGroup::groupExists(int groupId)
{
    return groups.find(groupId) != groups.end();
}

void PointToPointGroup::addGroup(int appId, int groupId, int groupSize)
{
    groups.emplace(std::make_pair(
      groupId, std::make_shared<PointToPointGroup>(appId, groupId, groupSize)));
}

void PointToPointGroup::clear()
{
    groups.clear();
}

PointToPointGroup::PointToPointGroup(int appIdIn,
                                     int groupIdIn,
                                     int groupSizeIn)
  : conf(faabric::util::getSystemConfig())
  , appId(appIdIn)
  , groupId(groupIdIn)
  , groupSize(groupSizeIn)
  , ptpBroker(faabric::transport::getPointToPointBroker())
{}

void PointToPointGroup::lock(int groupIdx, bool recursive)
{
    std::string host =
      ptpBroker.getHostForReceiver(groupId, POINT_TO_POINT_MASTER_IDX);

    if (host == conf.endpointHost) {
        masterLock(groupIdx, recursive);
    } else {
        auto cli = getClient(host);
        faabric::PointToPointMessage msg;
        msg.set_groupid(groupId);
        msg.set_sendidx(groupIdx);
        msg.set_recvidx(POINT_TO_POINT_MASTER_IDX);

        SPDLOG_TRACE("Remote lock {}:{}:{} to {}",
                     groupId,
                     groupIdx,
                     POINT_TO_POINT_MASTER_IDX,
                     host);

        cli->groupLock(appId, groupId, groupIdx, recursive);

        // Await ptp response
        ptpBroker.recvMessage(groupId, POINT_TO_POINT_MASTER_IDX, groupIdx);
    }
}

void PointToPointGroup::masterLock(int groupIdx, bool recursive)
{
    SPDLOG_TRACE("Master lock {}:{}", groupId, groupIdx);

    bool success = false;
    {
        faabric::util::UniqueLock lock(mx);
        if (recursive) {
            bool isFree = recursiveLockOwners.empty();

            bool getLockOwnerByMe =
              !isFree && (recursiveLockOwners.top() == groupIdx);

            if (isFree || getLockOwnerByMe) {
                // Recursive and either free, or already locked by this idx
                SPDLOG_TRACE("Group idx {} recursively locked {} ({})",
                             groupIdx,
                             groupId,
                             lockWaiters.size());
                recursiveLockOwners.push(groupIdx);
                success = true;
            } else {
                SPDLOG_TRACE("Group idx {} unable to recursively lock {} ({})",
                             groupIdx,
                             groupId,
                             lockWaiters.size());
            }
        } else if (!recursive && lockOwnerIdx == NO_LOCK_OWNER_IDX) {
            // Non-recursive and free
            SPDLOG_TRACE("Group idx {} locked {}", groupIdx, groupId);
            lockOwnerIdx = groupIdx;
            success = true;
        } else {
            // Unable to lock, wait in queue
            SPDLOG_TRACE("Group idx {} unable to lock {}", groupIdx, groupId);
            lockWaiters.push(groupIdx);
        }
    }

    if (success) {
        notifyLocked(groupIdx);
    }
}

void PointToPointGroup::localLock()
{
    LOCK_TIMEOUT(localMx, timeoutMs);
}

bool PointToPointGroup::localTryLock()
{
    SPDLOG_TRACE("Trying local lock on {}", groupId);
    return localMx.try_lock();
}

void PointToPointGroup::unlock(int groupIdx, bool recursive)
{
    std::string host =
      ptpBroker.getHostForReceiver(groupId, POINT_TO_POINT_MASTER_IDX);

    if (host == conf.endpointHost) {
        masterUnlock(groupIdx, recursive);
    } else {
        auto cli = getClient(host);
        faabric::PointToPointMessage msg;
        msg.set_groupid(groupId);
        msg.set_sendidx(groupIdx);
        msg.set_recvidx(POINT_TO_POINT_MASTER_IDX);

        SPDLOG_TRACE("Remote lock {}:{}:{} to {}",
                     groupId,
                     groupIdx,
                     POINT_TO_POINT_MASTER_IDX,
                     host);

        cli->groupUnlock(appId, groupId, groupIdx, recursive);
    }
}

void PointToPointGroup::masterUnlock(int groupIdx, bool recursive)
{
    faabric::util::UniqueLock lock(mx);

    if (recursive) {
        recursiveLockOwners.pop();

        if (!recursiveLockOwners.empty()) {
            return;
        }

        if (!lockWaiters.empty()) {
            recursiveLockOwners.push(lockWaiters.front());
            notifyLocked(lockWaiters.front());
            lockWaiters.pop();
        }
    } else {
        lockOwnerIdx = NO_LOCK_OWNER_IDX;

        if (!lockWaiters.empty()) {
            lockOwnerIdx = lockWaiters.front();
            notifyLocked(lockWaiters.front());
            lockWaiters.pop();
        }
    }
}

void PointToPointGroup::localUnlock()
{
    localMx.unlock();
}

void PointToPointGroup::notifyLocked(int groupIdx)
{
    std::vector<uint8_t> data(1, 0);

    ptpBroker.sendMessage(
      groupId, POINT_TO_POINT_MASTER_IDX, groupIdx, data.data(), data.size());
}

void PointToPointGroup::barrier(int groupIdx)
{
    // TODO more efficient barrier implementation to avoid load on the master
    if (groupIdx == POINT_TO_POINT_MASTER_IDX) {
        // Receive from all
        for (int i = 1; i < groupSize; i++) {
            ptpBroker.recvMessage(groupId, i, POINT_TO_POINT_MASTER_IDX);
        }

        // Reply to all
        std::vector<uint8_t> data(1, 0);
        for (int i = 1; i < groupSize; i++) {
            ptpBroker.sendMessage(
              groupId, POINT_TO_POINT_MASTER_IDX, i, data.data(), data.size());
        }
    } else {
        // Do the send
        std::vector<uint8_t> data(1, 0);
        ptpBroker.sendMessage(groupId,
                              groupIdx,
                              POINT_TO_POINT_MASTER_IDX,
                              data.data(),
                              data.size());

        // Await the response
        ptpBroker.recvMessage(groupId, POINT_TO_POINT_MASTER_IDX, groupIdx);
    }
}

void PointToPointGroup::notify(int groupIdx)
{
    if (groupIdx == POINT_TO_POINT_MASTER_IDX) {
        for (int i = 1; i < groupSize; i++) {
            ptpBroker.recvMessage(groupId, i, POINT_TO_POINT_MASTER_IDX);
        }
    } else {
        std::vector<uint8_t> data(1, 0);
        ptpBroker.sendMessage(groupId,
                              groupIdx,
                              POINT_TO_POINT_MASTER_IDX,
                              data.data(),
                              data.size());
    }
}

int PointToPointGroup::getLockOwner(bool recursive)
{
    if (recursive) {
        if (!recursiveLockOwners.empty()) {
            return recursiveLockOwners.top();
        }

        return NO_LOCK_OWNER_IDX;
    }

    return lockOwnerIdx;
}

PointToPointBroker::PointToPointBroker()
  : conf(faabric::util::getSystemConfig())
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

    // Prepare set of hosts in these mappings
    std::set<std::string> hosts;

    {
        faabric::util::FullLock lock(brokerMutex);

        // Set up the mappings
        for (int i = 0; i < decision.nFunctions; i++) {
            int groupIdx = decision.groupIdxs.at(i);
            const std::string& host = decision.hosts.at(i);

            SPDLOG_DEBUG("Setting point-to-point mapping {}:{}:{} on {}",
                         decision.appId,
                         groupId,
                         groupIdx,
                         host);

            // Record this index for this group
            groupIdIdxsMap[groupId].insert(groupIdx);

            // Add host mapping
            std::string key = getPointToPointKey(groupId, groupIdx);
            mappings[key] = host;

            // If it's not this host, add to set of returned hosts
            if (host != conf.endpointHost) {
                hosts.insert(host);
            }
        }

        // Register the group
        PointToPointGroup::addGroup(
          decision.appId, groupId, decision.nFunctions);
    }

    {
        // Lock this group
        faabric::util::UniqueLock lock(groupMappingMutexes[groupId]);

        SPDLOG_TRACE(
          "Enabling point-to-point mapping for {}:{}", decision.appId, groupId);

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
    // Set up locally
    std::set<std::string> otherHosts =
      setUpLocalMappingsFromSchedulingDecision(decision);

    // Send out to other hosts
    for (const auto& host : otherHosts) {
        faabric::PointToPointMappings msg;
        msg.set_appid(decision.appId);
        msg.set_groupid(decision.groupId);

        std::set<int>& indexes = groupIdIdxsMap[decision.groupId];

        for (int i = 0; i < decision.nFunctions; i++) {
            auto* mapping = msg.add_mappings();
            mapping->set_host(decision.hosts.at(i));
            mapping->set_messageid(decision.messageIds.at(i));
            mapping->set_appidx(decision.appIdxs.at(i));
            mapping->set_groupidx(decision.groupIdxs.at(i));
        }

        SPDLOG_DEBUG("Sending {} point-to-point mappings for {} to {}",
                     indexes.size(),
                     decision.groupId,
                     host);

        auto cli = getClient(host);
        cli->sendMappings(msg);
    }
}

void PointToPointBroker::waitForMappingsOnThisHost(int groupId)
{
    // Check if it's been enabled
    if (!groupMappingsFlags[groupId]) {

        // Lock this group
        faabric::util::UniqueLock lock(groupMappingMutexes[groupId]);

        // Check again
        if (!groupMappingsFlags[groupId]) {
            // Wait for group to be enabled
            auto timePoint = std::chrono::system_clock::now() +
                             std::chrono::milliseconds(MAPPING_TIMEOUT_MS);

            if (!groupMappingCvs[groupId].wait_until(
                  lock, timePoint, [this, groupId] {
                      return groupMappingsFlags[groupId];
                  })) {

                SPDLOG_ERROR("Timed out waiting for group mappings {}",
                             groupId);
                throw std::runtime_error(
                  "Timed out waiting for group mappings");
            }

            SPDLOG_TRACE("Point-to-point mappings for {} ready", groupId);
        }
    }
}

std::set<int> PointToPointBroker::getIdxsRegisteredForGroup(int groupId)
{
    faabric::util::SharedLock lock(brokerMutex);
    return groupIdIdxsMap[groupId];
}

void PointToPointBroker::sendMessage(int groupId,
                                     int sendIdx,
                                     int recvIdx,
                                     const uint8_t* buffer,
                                     size_t bufferSize)
{
    waitForMappingsOnThisHost(groupId);

    std::string host = getHostForReceiver(groupId, recvIdx);

    if (host == conf.endpointHost) {
        std::string label = getPointToPointKey(groupId, sendIdx, recvIdx);

        // Note - this map is thread-local so no locking required
        if (sendEndpoints.find(label) == sendEndpoints.end()) {
            sendEndpoints[label] =
              std::make_unique<AsyncInternalSendMessageEndpoint>(label);

            SPDLOG_TRACE("Created new internal send endpoint {}",
                         sendEndpoints[label]->getAddress());
        }

        SPDLOG_TRACE("Local point-to-point message {}:{}:{} to {}",
                     groupId,
                     sendIdx,
                     recvIdx,
                     sendEndpoints[label]->getAddress());

        sendEndpoints[label]->send(buffer, bufferSize);

    } else {
        auto cli = getClient(host);
        faabric::PointToPointMessage msg;
        msg.set_groupid(groupId);
        msg.set_sendidx(sendIdx);
        msg.set_recvidx(recvIdx);
        msg.set_data(buffer, bufferSize);

        SPDLOG_TRACE("Remote point-to-point message {}:{}:{} to {}",
                     groupId,
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

void PointToPointBroker::clear()
{
    faabric::util::SharedLock lock(brokerMutex);

    groupIdIdxsMap.clear();
    mappings.clear();

    PointToPointGroup::clear();

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
    static PointToPointBroker broker;
    return broker;
}
}
