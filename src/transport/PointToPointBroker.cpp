#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/util/bytes.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <list>

#define NO_LOCK_OWNER_IDX -1

#define MAPPING_TIMEOUT_MS 20000

#define NO_CURRENT_GROUP_ID -1

namespace faabric::transport {

static std::unordered_map<int, std::shared_ptr<PointToPointGroup>> groups;

static std::shared_mutex groupsMutex;

// Keeping 0MQ sockets in TLS is usually a bad idea, as they _must_ be closed
// before the global context. However, in this case it's worth it to cache the
// sockets across messages, as otherwise we'd be creating and destroying a lot
// of them under high throughput. To ensure things are cleared up, see the
// thread-local tidy-up message on this class and its usage in the rest of the
// codebase.
thread_local std::
  unordered_map<std::string, std::unique_ptr<AsyncInternalRecvMessageEndpoint>>
    recvEndpoints;

thread_local std::
  unordered_map<std::string, std::unique_ptr<AsyncInternalSendMessageEndpoint>>
    sendEndpoints;

thread_local std::unordered_map<std::string,
                                std::shared_ptr<PointToPointClient>>
  clients;

// Thread local data structures for in-order message delivery
thread_local int currentGroupId = NO_CURRENT_GROUP_ID;

// On the sent message count we index by receiving rank and on the receive
// message count we index by sending rank
thread_local std::vector<int> sentMsgCount;

thread_local std::vector<int> recvMsgCount;

thread_local std::vector<std::list<Message>> outOfOrderMsgs;

static std::shared_ptr<PointToPointClient> getClient(const std::string& host)
{
    // This map is thread-local so no locking required
    if (clients.find(host) == clients.end()) {
        clients.insert(
          std::pair<std::string, std::shared_ptr<PointToPointClient>>(
            host, std::make_shared<PointToPointClient>(host)));

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
    faabric::util::SharedLock lock(groupsMutex);

    if (groups.find(groupId) == groups.end()) {
        SPDLOG_ERROR("Did not find group ID {} on this host", groupId);
        throw std::runtime_error("Group ID not found on host");
    }

    return groups.at(groupId);
}

std::shared_ptr<PointToPointGroup> PointToPointGroup::getOrAwaitGroup(
  int groupId)
{
    getPointToPointBroker().waitForMappingsOnThisHost(groupId);

    return getGroup(groupId);
}

bool PointToPointGroup::groupExists(int groupId)
{
    faabric::util::SharedLock lock(groupsMutex);
    return groups.find(groupId) != groups.end();
}

void PointToPointGroup::addGroup(int appId, int groupId, int groupSize)
{
    faabric::util::FullLock lock(groupsMutex);

    if (groups.find(groupId) == groups.end()) {
        groups.emplace(std::make_pair(
          groupId,
          std::make_shared<PointToPointGroup>(appId, groupId, groupSize)));
    }
}

void PointToPointGroup::addGroupIfNotExists(int appId,
                                            int groupId,
                                            int groupSize)
{
    if (groupExists(groupId)) {
        return;
    }

    addGroup(appId, groupId, groupSize);
}

void PointToPointGroup::clearGroup(int groupId)
{
    faabric::util::FullLock lock(groupsMutex);

    groups.erase(groupId);
}

void PointToPointGroup::clear()
{
    faabric::util::FullLock lock(groupsMutex);

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
    std::string masterHost =
      ptpBroker.getHostForReceiver(groupId, POINT_TO_POINT_MASTER_IDX);
    std::string lockerHost = ptpBroker.getHostForReceiver(groupId, groupIdx);

    bool masterIsLocal = masterHost == conf.endpointHost;
    bool lockerIsLocal = lockerHost == conf.endpointHost;

    // If we're on the master, we need to try and acquire the lock, otherwise we
    // send a remote request
    if (masterIsLocal) {
        bool acquiredLock = false;
        {
            faabric::util::FullLock lock(mx);

            if (recursive && (recursiveLockOwners.empty() ||
                              recursiveLockOwners.top() == groupIdx)) {
                // Recursive and either free, or already locked by this idx
                recursiveLockOwners.push(groupIdx);
                acquiredLock = true;
            } else if (!recursive &&
                       (lockOwnerIdx.load(std::memory_order_acquire) ==
                        NO_LOCK_OWNER_IDX)) {
                // Non-recursive and free
                lockOwnerIdx.store(groupIdx, std::memory_order_release);
                acquiredLock = true;
            }
        }

        if (acquiredLock && lockerIsLocal) {
            // Nothing to do now
            SPDLOG_TRACE("Group idx {} ({}), locally locked {} (recursive {})",
                         groupIdx,
                         lockerHost,
                         groupId,
                         recursive);

        } else if (acquiredLock) {
            SPDLOG_TRACE("Group idx {} ({}), remotely locked {} (recursive {})",
                         groupIdx,
                         lockerHost,
                         groupId,
                         recursive);

            // Notify remote locker that they've acquired the lock
            notifyLocked(groupIdx);
        } else {
            {
                faabric::util::FullLock lock(mx);
                // Need to wait to get the lock
                lockWaiters.push(groupIdx);
            }

            // Wait here if local, otherwise the remote end will pick up the
            // message
            if (lockerIsLocal) {
                SPDLOG_TRACE(
                  "Group idx {} ({}), locally awaiting lock {} (recursive {})",
                  groupIdx,
                  lockerHost,
                  groupId,
                  recursive);

                ptpBroker.recvMessage(
                  groupId, POINT_TO_POINT_MASTER_IDX, groupIdx);
            } else {
                // Notify remote locker that they've acquired the lock
                SPDLOG_TRACE(
                  "Group idx {} ({}), remotely awaiting lock {} (recursive {})",
                  groupIdx,
                  lockerHost,
                  groupId,
                  masterHost,
                  recursive);
            }
        }
    } else {
        auto cli = getClient(masterHost);
        faabric::PointToPointMessage msg;
        msg.set_groupid(groupId);
        msg.set_sendidx(groupIdx);
        msg.set_recvidx(POINT_TO_POINT_MASTER_IDX);

        SPDLOG_TRACE("Remote lock {}:{}:{} to {}",
                     groupId,
                     groupIdx,
                     POINT_TO_POINT_MASTER_IDX,
                     masterHost);

        // Send the remote request and await the message saying it's been
        // acquired
        cli->groupLock(appId, groupId, groupIdx, recursive);

        ptpBroker.recvMessage(groupId, POINT_TO_POINT_MASTER_IDX, groupIdx);
    }
}

void PointToPointGroup::localLock()
{
    localMx.lock();
}

bool PointToPointGroup::localTryLock()
{
    return localMx.try_lock();
}

void PointToPointGroup::unlock(int groupIdx, bool recursive)
{
    std::string host =
      ptpBroker.getHostForReceiver(groupId, POINT_TO_POINT_MASTER_IDX);

    if (host == conf.endpointHost) {
        faabric::util::FullLock lock(mx);

        SPDLOG_TRACE("Group idx {} unlocking {} ({} waiters, recursive {})",
                     groupIdx,
                     groupId,
                     lockWaiters.size(),
                     recursive);

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
            if (!lockWaiters.empty()) {
                lockOwnerIdx.store(lockWaiters.front(),
                                   std::memory_order_release);
                notifyLocked(lockWaiters.front());
                lockWaiters.pop();
            } else {
                lockOwnerIdx.store(NO_LOCK_OWNER_IDX,
                                   std::memory_order_release);
            }
        }
    } else {
        auto cli = getClient(host);
        faabric::PointToPointMessage msg;
        msg.set_groupid(groupId);
        msg.set_sendidx(groupIdx);
        msg.set_recvidx(POINT_TO_POINT_MASTER_IDX);

        SPDLOG_TRACE("Remote unlock {}:{}:{} to {}",
                     groupId,
                     groupIdx,
                     POINT_TO_POINT_MASTER_IDX,
                     host);

        cli->groupUnlock(appId, groupId, groupIdx, recursive);
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
            SPDLOG_TRACE(
              "Master group {} waiting for notify from index {}", groupId, i);

            ptpBroker.recvMessage(groupId, i, POINT_TO_POINT_MASTER_IDX);

            SPDLOG_TRACE("Master group {} notified by index {}", groupId, i);
        }
    } else {
        std::vector<uint8_t> data(1, 0);
        SPDLOG_TRACE("Notifying group {} from index {}", groupId, groupIdx);
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
        faabric::util::SharedLock lock(mx);
        if (!recursiveLockOwners.empty()) {
            return recursiveLockOwners.top();
        }

        return NO_LOCK_OWNER_IDX;
    }

    return lockOwnerIdx.load(std::memory_order_acquire);
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

    SPDLOG_TRACE(
      "Enabling point-to-point mapping for {}:{}", decision.appId, groupId);

    getGroupFlag(groupId)->setFlag(true);

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

std::shared_ptr<faabric::util::FlagWaiter> PointToPointBroker::getGroupFlag(
  int groupId)
{
    faabric::util::SharedLock lock(brokerMutex);
    if (groupFlags.find(groupId) == groupFlags.end()) {
        lock.unlock();
        faabric::util::FullLock lock(brokerMutex);
        if (groupFlags.find(groupId) == groupFlags.end()) {
            return groupFlags
              .emplace(groupId, std::make_shared<faabric::util::FlagWaiter>())
              .first->second;
        }
    }

    return groupFlags.at(groupId);
}

void PointToPointBroker::waitForMappingsOnThisHost(int groupId)
{
    auto waiter = getGroupFlag(groupId);

    // Check if it's been enabled
    waiter->waitOnFlag();
}

std::set<int> PointToPointBroker::getIdxsRegisteredForGroup(int groupId)
{
    faabric::util::SharedLock lock(brokerMutex);
    return groupIdIdxsMap[groupId];
}

void PointToPointBroker::initSequenceCounters(int groupId)
{
    if (currentGroupId != NO_CURRENT_GROUP_ID) {
        SPDLOG_DEBUG("Changing the current group Id in PTP broker ({} -> {})",
                     currentGroupId,
                     groupId);
    }
    currentGroupId = groupId;
    int groupSize = getIdxsRegisteredForGroup(groupId).size();
    // We initialise both counters at the same time, as we only know once per
    // thread when we have changed group id
    sentMsgCount = std::vector<int>(groupSize, 0);
    recvMsgCount = std::vector<int>(groupSize, 0);
    outOfOrderMsgs.resize(groupSize);
}

int PointToPointBroker::getAndIncrementSentMsgCount(int groupId, int recvIdx)
{
    if (groupId != currentGroupId) {
        initSequenceCounters(groupId);
    }

    int retSeqNum = sentMsgCount.at(recvIdx)++;

    return retSeqNum;
}

int PointToPointBroker::getExpectedSeqNum(int groupId, int sendIdx)
{
    if (groupId != currentGroupId) {
        initSequenceCounters(groupId);
    }

    return recvMsgCount.at(sendIdx);
}

void PointToPointBroker::incrementRecvMsgCount(int groupId, int sendIdx)
{
    if (groupId != currentGroupId) {
        initSequenceCounters(groupId);
    }

    recvMsgCount.at(sendIdx)++;
}

void PointToPointBroker::updateHostForIdx(int groupId,
                                          int groupIdx,
                                          std::string newHost)
{
    faabric::util::FullLock lock(brokerMutex);

    std::string key = getPointToPointKey(groupId, groupIdx);

    SPDLOG_DEBUG("Updating point-to-point mapping for {}:{} from {} to {}",
                 groupId,
                 groupIdx,
                 mappings[key],
                 newHost);

    mappings[key] = newHost;
}

void PointToPointBroker::sendMessage(int groupId,
                                     int sendIdx,
                                     int recvIdx,
                                     const uint8_t* buffer,
                                     size_t bufferSize,
                                     std::string hostHint,
                                     bool mustOrderMsg)
{
    sendMessage(groupId,
                sendIdx,
                recvIdx,
                buffer,
                bufferSize,
                mustOrderMsg,
                NO_SEQUENCE_NUM,
                hostHint);
}

void PointToPointBroker::sendMessage(int groupId,
                                     int sendIdx,
                                     int recvIdx,
                                     const uint8_t* buffer,
                                     size_t bufferSize,
                                     bool mustOrderMsg,
                                     int sequenceNum,
                                     std::string hostHint)
{
    // When sending a remote message, this method is called once from the
    // sender thread, and another time from the point-to-point server to route
    // it to the receiver thread

    waitForMappingsOnThisHost(groupId);

    // If the application code knows which host does the receiver live in
    // (cached for performance) we allow it to provide a hint to avoid
    // acquiring a shared lock here
    std::string host =
      hostHint.empty() ? getHostForReceiver(groupId, recvIdx) : hostHint;

    // Set the sequence number if we need ordering and one is not provided
    bool mustSetSequenceNum = mustOrderMsg && sequenceNum == NO_SEQUENCE_NUM;

    if (host == conf.endpointHost) {
        std::string label = getPointToPointKey(groupId, sendIdx, recvIdx);

        // This map is thread-local so no locking required
        if (sendEndpoints.find(label) == sendEndpoints.end()) {
            sendEndpoints[label] =
              std::make_unique<AsyncInternalSendMessageEndpoint>(label);

            SPDLOG_TRACE("Created new internal send endpoint {}",
                         sendEndpoints[label]->getAddress());
        }

        // When sending a local message, if called from the PTP server we
        // forward whatever sequence number the server passed, if called from
        // the sender thread we add a sequence number (if needed)
        int localSendSeqNum = sequenceNum;
        if (mustSetSequenceNum) {
            localSendSeqNum = getAndIncrementSentMsgCount(groupId, recvIdx);
        }

        SPDLOG_TRACE("Local point-to-point message {}:{}:{} (seq: {}) to {}",
                     groupId,
                     sendIdx,
                     recvIdx,
                     localSendSeqNum,
                     sendEndpoints[label]->getAddress());

        sendEndpoints[label]->send(
          NO_HEADER, buffer, bufferSize, localSendSeqNum);

    } else {
        auto cli = getClient(host);
        faabric::PointToPointMessage msg;
        msg.set_groupid(groupId);
        msg.set_sendidx(sendIdx);
        msg.set_recvidx(recvIdx);
        msg.set_data(buffer, bufferSize);

        // When sending a remote message, we set a sequence number if required
        int remoteSendSeqNum = NO_SEQUENCE_NUM;
        if (mustSetSequenceNum) {
            remoteSendSeqNum = getAndIncrementSentMsgCount(groupId, recvIdx);
        }

        SPDLOG_TRACE("Remote point-to-point message {}:{}:{} (seq: {}) to {}",
                     groupId,
                     sendIdx,
                     recvIdx,
                     remoteSendSeqNum,
                     host);

        cli->sendMessage(msg, remoteSendSeqNum);
    }
}

Message PointToPointBroker::doRecvMessage(int groupId, int sendIdx, int recvIdx)
{
    std::string label = getPointToPointKey(groupId, sendIdx, recvIdx);

    // Note: this map is thread-local so no locking required
    if (recvEndpoints.find(label) == recvEndpoints.end()) {
        recvEndpoints[label] =
          std::make_unique<AsyncInternalRecvMessageEndpoint>(label);
        SPDLOG_TRACE("Created new internal recv endpoint {}",
                     recvEndpoints[label]->getAddress());
    }

    return recvEndpoints[label]->recv();
}

std::vector<uint8_t> PointToPointBroker::recvMessage(int groupId,
                                                     int sendIdx,
                                                     int recvIdx,
                                                     bool mustOrderMsg)
{
    // If we don't need to receive messages in order, return here
    if (!mustOrderMsg) {
        // TODO - can we avoid this copy?
        return doRecvMessage(groupId, sendIdx, recvIdx).dataCopy();
    }

    // Get the sequence number we expect to receive
    int expectedSeqNum = getExpectedSeqNum(groupId, sendIdx);

    // We first check if we have already received the message. We only need to
    // check this once.
    auto foundIterator =
      std::find_if(outOfOrderMsgs.at(sendIdx).begin(),
                   outOfOrderMsgs.at(sendIdx).end(),
                   [expectedSeqNum](const Message& msg) {
                       return msg.getSequenceNum() == expectedSeqNum;
                   });
    if (foundIterator != outOfOrderMsgs.at(sendIdx).end()) {
        SPDLOG_TRACE("Retrieved the expected message ({}:{} seq: {}) from the "
                     "out-of-order buffer",
                     sendIdx,
                     recvIdx,
                     expectedSeqNum);
        incrementRecvMsgCount(groupId, sendIdx);
        Message returnMsg = std::move(*foundIterator);
        outOfOrderMsgs.at(sendIdx).erase(foundIterator);
        return returnMsg.dataCopy();
    }

    // Given that we don't have the message, we query the transport layer until
    // we receive it
    while (true) {
        SPDLOG_TRACE(
          "Entering loop to query transport layer for msg ({}:{} seq: {})",
          sendIdx,
          recvIdx,
          expectedSeqNum);
        // Receive from the transport layer
        Message recvMsg = doRecvMessage(groupId, sendIdx, recvIdx);

        // If the sequence numbers match, exit the loop
        int seqNum = recvMsg.getSequenceNum();
        if (seqNum == expectedSeqNum) {
            SPDLOG_TRACE("Received the expected message ({}:{} seq: {})",
                         sendIdx,
                         recvIdx,
                         expectedSeqNum);
            incrementRecvMsgCount(groupId, sendIdx);
            return recvMsg.dataCopy();
        }

        // If not, we must insert the received message in the out of order
        // received messages
        SPDLOG_TRACE("Received out-of-order message ({}:{} seq: {}) (expected: "
                     "{} - got: {})",
                     sendIdx,
                     recvIdx,
                     seqNum,
                     expectedSeqNum,
                     seqNum);
        outOfOrderMsgs.at(sendIdx).emplace_back(std::move(recvMsg));
    }
}

void PointToPointBroker::clearGroup(int groupId)
{
    SPDLOG_TRACE("Clearing point-to-point group {}", groupId);

    faabric::util::FullLock lock(brokerMutex);

    std::set<int> idxs = getIdxsRegisteredForGroup(groupId);
    for (auto idxA : idxs) {
        for (auto idxB : idxs) {
            std::string label = getPointToPointKey(groupId, idxA, idxB);
            mappings.erase(label);
        }
    }

    groupIdIdxsMap.erase(groupId);

    PointToPointGroup::clearGroup(groupId);

    groupFlags.erase(groupId);
}

void PointToPointBroker::clear()
{
    faabric::util::FullLock lock(brokerMutex);

    groupIdIdxsMap.clear();
    mappings.clear();

    PointToPointGroup::clear();

    groupFlags.clear();
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
