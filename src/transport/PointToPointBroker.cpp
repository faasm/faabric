#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/Message.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/util/bytes.h>
#include <faabric/util/concurrent_map.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <absl/container/flat_hash_set.h>
#include <atomic>
#include <list>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>

#define NO_LOCK_OWNER_IDX -1

#define MAPPING_TIMEOUT_MS 20000

#define NO_CURRENT_GROUP_ID -1

namespace faabric::transport {

static faabric::util::ConcurrentMap<int, std::shared_ptr<PointToPointGroup>>
  groups;

// To ensure things are cleared up, see the thread-local tidy-up message on this
// class and its usage in the rest of the codebase, the atomic int counts
// threads with active references to an endpoint pair.
static faabric::util::ConcurrentMap<
  std::string,
  std::shared_ptr<std::tuple<std::unique_ptr<AsyncInternalRecvMessageEndpoint>,
                             std::unique_ptr<AsyncInternalSendMessageEndpoint>,
                             std::atomic_int32_t>>>
  endpoints;
thread_local absl::flat_hash_set<std::string> threadEndpoints;

static faabric::util::ConcurrentMap<std::string,
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
    auto client = clients.get(host).value_or(nullptr);
    if (client == nullptr) {
        client = clients.tryEmplaceShared(host, host).second;
        SPDLOG_TRACE("Created new point-to-point client {}", host);
    }
    return client;
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
    auto group = groups.get(groupId);

    if (!group.has_value()) {
        SPDLOG_ERROR("Did not find group ID {} on this host", groupId);
        throw std::runtime_error("Group ID not found on host");
    }

    return *group;
}

std::shared_ptr<PointToPointGroup> PointToPointGroup::getOrAwaitGroup(
  int groupId)
{
    getPointToPointBroker().waitForMappingsOnThisHost(groupId);

    return getGroup(groupId);
}

bool PointToPointGroup::groupExists(int groupId)
{
    return groups.contains(groupId);
}

void PointToPointGroup::addGroup(int appId, int groupId, int groupSize, int numLocalIdxs)
{
    groups.tryEmplaceShared(groupId, appId, groupId, groupSize);
}

void PointToPointGroup::addGroupIfNotExists(int appId,
                                            int groupId,
                                            int groupSize)
{
    addGroup(appId, groupId, groupSize);
}

void PointToPointGroup::clearGroup(int groupId)
{
    groups.erase(groupId);
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
{
}

// TODO: use the previous constructor
PointToPointGroup::PointToPointGroup(int appIdIn,
                                     int groupIdIn,
                                     int groupSizeIn,
                                     int numLocalIdxsIn)
  : conf(faabric::util::getSystemConfig())
  , appId(appIdIn)
  , groupId(groupIdIn)
  , groupSize(groupSizeIn)
  , numLocalIdxs(numLocalIdxsIn)
  , ptpBroker(faabric::transport::getPointToPointBroker())
{
    localBarrierPrivate = faabric::util::Barrier::create(numLocalIdxsIn);
}

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

void PointToPointGroup::localBarrier()
{
    if (localBarrierPrivate == nullptr) {
        SPDLOG_ERROR("Local barrier for group {} is not initialised", groupId);
        throw std::runtime_error("Local barrier is not initialised");
    }

    localBarrierPrivate->wait();
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

        int numLocalIdxs = 0;

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
            } else {
                numLocalIdxs++;
            }
        }

        // Register the group
        PointToPointGroup::addGroup(
          decision.appId, groupId, decision.nFunctions, numLocalIdxs);
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

    /* TODO - update the number of local indexes
    if (newHost == conf.endpointHost) {
        numLocalIdxs += 1;
        localBarrierPrivate = faabric::util::Barrier::create(numLocalIdxs);
    }
    */
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

// Gets or creates a pair of inproc endpoints (recv&send) in the endpoints map.
// Ensures the receiving endpoint gets created first. A reference counter is
// also allocated with the pair to keep track of how many threads are using the
// endpoint pair for cleanup later.
auto getEndpointPtrs(const std::string& label)
{
    auto maybeEndpoint = endpoints.get(label);
    std::shared_ptr endpointPtrs = maybeEndpoint.value_or(nullptr);
    if (!maybeEndpoint.has_value()) {
        endpointPtrs = endpoints.tryEmplaceThenMutate(
          label,
          [&]<class T>(bool inserted, std::shared_ptr<T>& ptr) {
              if (inserted) {
                  ptr = std::make_shared<T>(nullptr, nullptr, 0);
                  // Tuple construction order is unspecified, so make sure to
                  // create recv before send.
                  std::get<0>(*ptr) =
                    std::make_unique<AsyncInternalRecvMessageEndpoint>(label);
                  std::get<1>(*ptr) =
                    std::make_unique<AsyncInternalSendMessageEndpoint>(label);
                  SPDLOG_TRACE("Created new internal endpoints: {}",
                               std::get<0>(*ptr)->getAddress());
              }
              // Ensure a copy and not a reference is returned
              return std::shared_ptr(ptr);
          },
          nullptr);
    }
    auto& refcount = std::get<std::atomic_int32_t>(*endpointPtrs);
    if (threadEndpoints.emplace(label).second) {
        refcount.fetch_add(1);
    }
    return endpointPtrs;
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

        auto endpointPtrs = getEndpointPtrs(label);
        auto& endpoint =
          *std::get<std::unique_ptr<AsyncInternalSendMessageEndpoint>>(
            *endpointPtrs);

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
                     endpoint.getAddress());

        endpoint.send(NO_HEADER, buffer, bufferSize, localSendSeqNum);

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

    auto endpointPtrs = getEndpointPtrs(label);
    auto& endpoint =
      *std::get<std::unique_ptr<AsyncInternalRecvMessageEndpoint>>(
        *endpointPtrs);

    return endpoint.recv();
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

        // If the receive was not successful, exit the loop
        if (recvMsg.getResponseCode() !=
            faabric::transport::MessageResponseCode::SUCCESS) {
            SPDLOG_WARN(
              "Error {} when awaiting a message ({}:{} seq: {} label: {})",
              static_cast<int>(recvMsg.getResponseCode()),
              sendIdx,
              recvIdx,
              expectedSeqNum,
              getPointToPointKey(groupId, sendIdx, recvIdx));
            throw std::runtime_error("Error when awaiting a PTP message");
        }

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
    for (const std::string& key : threadEndpoints) {
        auto maybeEndpointPtrs = endpoints.get(key);
        if (maybeEndpointPtrs.has_value()) {
            auto& endpointPtrs = *maybeEndpointPtrs;
            auto& refcount = std::get<std::atomic_int32_t>(*endpointPtrs);
            if (refcount.fetch_sub(1) <= 1) {
                endpoints.erase(key);
            }
        }
    }
    threadEndpoints.clear();
}

std::vector<std::pair<std::string, int>> PointToPointBroker::sortGroupHostsByFrequency(int groupId)
{
    faabric::util::SharedLock lock(brokerMutex);

    std::vector<std::pair<std::string, int>> sortedHosts;

    // If the group does not exist, return an empty list
    if (!PointToPointGroup::groupExists(groupId)) {
        SPDLOG_WARN("Group {} does not exist", groupId);
        return sortedHosts;
    }

    std::set<int> groupIdxs = getIdxsRegisteredForGroup(groupId);
    assert(groupIdxs.size() == groupSize);
    std::map<std::string, int> hostCount;

    for (const auto& idx : groupIdxs) {
        auto host = getHostForReceiver(groupId, idx);
        if (hostCount.find(host) == hostCount.end()) {
            hostCount[host] = 0;
        }
        hostCount.at(host) += 1;
        sortedHosts.push_back(std::make_pair(host, idx));
    }

    // Sort the pair list using the frequency map as helper
    std::sort(sortedHosts.begin(), sortedHosts.end(), [hostCount](std::pair<std::string, int> pairA, std::pair<std::string, int> pairB) {
        // If hosts are different, return the host with the highest frequency
        // count
        std::string hostA = pairA.first;
        std::string hostB = pairB.first;
        if (hostA != hostB) {
            return hostCount.at(hostA) > hostCount.at(hostB);
        }

        // If hosts are the same, put the smaller index first
        int idxA = pairA.second;
        int idxB = pairB.second;
        return idxA < idxB;
    });

    return sortedHosts;
}

// We need to update two kinds of records when migrating a function that uses
// the PTP broker: the host to idx mapping, and the sequence number counters
void PointToPointBroker::preMigrationHook(
  int groupId, int groupIdx, std::shared_ptr<faabric::PendingMigrations> pendingMigrations)
{
    // TODO: check for out-of-order messages
    if (pendingMigrations->groupid() != groupId) {
        SPDLOG_ERROR("make me an assertion");
        throw std::runtime_error("make me an assertion");
    }

    InFlightMigType inFlightMapPtr = nullptr;
    if (groupIdx == POINT_TO_POINT_MASTER_IDX) {
        faabric::util::FullLock lock(brokerMutex);

        if (inFlightMigrations.find(groupId) != inFlightMigrations.end()) {
            SPDLOG_ERROR("Migration already in flight for group {}!", groupId);
            throw std::runtime_error("Migration already in flight!");
        }

        inFlightMigrations[groupId] =
          std::make_shared<std::unordered_map<int, std::shared_ptr<faabric::MigratedFuncMetadata>>>();

        inFlightMapPtr = inFlightMigrations.at(groupId);
    }

    auto thisHost = conf.endpointHost;
    // TODO: we could pass a hint on whether this function must migrate or not
    for (const auto& m : pendingMigrations->migrations()) {
        // The send/recv counters are thread local, so we can only access them
        // from the right function in the right host
        if ((m.srchost() == thisHost) && m.msg().groupidx() == groupIdx) {
            // Send the main idx our send and recv counters
            faabric::MigratedFuncMetadata metadata;
            auto* migrationPtr = metadata.mutable_migration();
            *migrationPtr = m;

            // Copy the thread local counts into the message
            for (const auto& sentCount : sentMsgCount) {
                metadata.add_sentmsgcount(sentCount);
            }
            for (const auto& recvCount : recvMsgCount) {
                metadata.add_recvmsgcount(recvCount);
            }
            std::string buffer;
            if (!metadata.SerializeToString(&buffer)) {
                throw std::runtime_error("Error serialising message");
            }

            SPDLOG_DEBUG("Pre-migration: {}:{} sending function migration metadata to {}:{} (sent size: {} - recv size: {})",
                        groupId,
                        groupIdx,
                        groupId,
                        POINT_TO_POINT_MASTER_IDX,
                        metadata.sentmsgcount_size(),
                        metadata.recvmsgcount_size());
            sendMessage(groupId,
                        groupIdx,
                        POINT_TO_POINT_MASTER_IDX,
                        reinterpret_cast<uint8_t*>(buffer.data()),
                        buffer.size());

            break;
        }

        // If we are the main index, we receive metadata for _all_ migrated
        // funcs. Note that this means that we can not migrate the main idx
        if (groupIdx == POINT_TO_POINT_MASTER_IDX) {
            if (m.msg().mpirank() == groupIdx) {
                throw std::runtime_error("Can not migrate main PTP index!");
            }

            auto rawMsg = recvMessage(groupId, m.msg().mpirank(), groupIdx);
            PARSE_MSG(faabric::MigratedFuncMetadata, rawMsg.data(), rawMsg.size());

            if (inFlightMapPtr == nullptr) {
                throw std::runtime_error("making meh an assertion porfavor");
            }
            SPDLOG_DEBUG("Pre-migration: adding in-flight migration metadata for rank: {}", m.msg().mpirank());
            inFlightMapPtr->operator[](m.msg().mpirank()) =
              std::make_shared<faabric::MigratedFuncMetadata>(parsedMsg);
        }
    }

    // Hit a local-barrier before we exit the pre-migration hook so that after
    // the barrier it is safe to update the host mappings
    PointToPointGroup::getGroup(groupId)->localBarrier();
}

void PointToPointBroker::postMigrationHook(int groupId, int groupIdx)
{
    InFlightMigType inFlightMapPtr = nullptr;
    if (groupIdx == POINT_TO_POINT_MASTER_IDX) {
        faabric::util::FullLock lock(brokerMutex);

        if (inFlightMigrations.find(groupId) == inFlightMigrations.end()) {
            SPDLOG_ERROR("Migration not in flight during port-hook! ({})", groupId);
            throw std::runtime_error("Migration not in flight!");
        }

        inFlightMapPtr = inFlightMigrations.at(groupId);
    }

    if (groupIdx == POINT_TO_POINT_MASTER_IDX) {
        // If we are the main index, send the corresponding message to all ranks
        auto idxs = getIdxsRegisteredForGroup(groupId);
        for (const auto& idx : idxs) {
            std::string buffer;

            // If we have migration metadata, send it, otherwise send an empty
            // request
            if (inFlightMapPtr->find(idx) != inFlightMapPtr->end()) {
                if (!inFlightMapPtr->at(idx)->SerializeToString(&buffer)) {
                    throw std::runtime_error("Error serialising message");
                }
            } else {
                // TODO: could we avoid sending this placeholder?
                faabric::MigratedFuncMetadata placeholder;
                placeholder.SerializeToString(&buffer);
            }
            sendMessage(groupId,
                        groupIdx,
                        idx,
                        reinterpret_cast<uint8_t*>(buffer.data()),
                        buffer.size());
            inFlightMapPtr->erase(idx);
        }
        // Finally null-out the pointer and remove it from the map
        inFlightMapPtr = nullptr;
        {
            faabric::util::FullLock lock(brokerMutex);

            inFlightMigrations.erase(groupId);
        }
    } else {
        // If we are not the main index, receive the message from the main index
        // containing the migrated function metadata (or a placeholder)
        auto rawMsg = recvMessage(groupId, POINT_TO_POINT_MASTER_IDX, groupIdx);
        PARSE_MSG(faabric::MigratedFuncMetadata, rawMsg.data(), rawMsg.size());

        if (parsedMsg.sentmsgcount_size() != 0) {
            if (groupId != currentGroupId) {
                initSequenceCounters(groupId);
            }
            SPDLOG_INFO("Post-migration: updating message counters for {}:{}", groupId, groupIdx);
        } else {
            SPDLOG_INFO("Post-migration: NOT updating message counters for {}:{}", groupId, groupIdx);
        }

        for (int i = 0; i < parsedMsg.sentmsgcount_size(); i++) {
            sentMsgCount.at(i) = parsedMsg.sentmsgcount().at(i);
        }
        for (int i = 0; i < parsedMsg.recvmsgcount_size(); i++) {
            recvMsgCount.at(i) = parsedMsg.recvmsgcount().at(i);
        }
    }

    // Do we need a barrier here just in case?
    // TODO: maybe this is a bit too cautious?
    /*
    SPDLOG_INFO("[{}] {}:{} entering last-most post-migration barrier",
                conf.endpointHost, groupId, groupIdx);
    PointToPointGroup::getGroup(groupId)->barrier(groupIdx);
    SPDLOG_INFO("[{}] {}:{} exitting last-most post-migration barrier",
                conf.endpointHost, groupId, groupIdx);
    */
    // TODO - we should probably remove the pending migration here?
}


PointToPointBroker& getPointToPointBroker()
{
    static PointToPointBroker broker;
    return broker;
}
}
