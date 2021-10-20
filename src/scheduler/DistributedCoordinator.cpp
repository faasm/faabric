#include <faabric/scheduler/DistributedCoordinator.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/barrier.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#define GROUP_TIMEOUT_MS 20000

#define LOCK_TIMEOUT(mx, ms)                                                   \
    auto timePoint =                                                           \
      std::chrono::system_clock::now() + std::chrono::milliseconds(ms);        \
    bool success = mx.try_lock_until(timePoint);                               \
    if (!success) {                                                            \
        throw std::runtime_error("Distributed coordination timeout");          \
    }

namespace faabric::scheduler {

DistributedLock::DistributedLock(int32_t groupIdIn, bool recursiveIn)
  : groupId(groupIdIn)
  , ptpBroker(faabric::transport::getPointToPointBroker())
  , recursive(recursiveIn)
{}

void DistributedLock::tryLock(int32_t memberIdx)
{
    faabric::util::UniqueLock lock(mx);

    bool success = false;

    if (ownerIdx == -1) {
        success = true;
    } else if ((ownerIdx == memberIdx) && recursive) {
        success = true;
    } else {
        success = false;
    }

    if (success) {
        ownerIdx = memberIdx;

        SPDLOG_TRACE("Member {} locked {}", memberIdx, groupId, ownerIdx);

        notifyLocked(ownerIdx);
    } else {
        SPDLOG_TRACE("Member {} waiting on lock {}. Already locked by {}",
                     memberIdx,
                     groupId,
                     ownerIdx);

        // Add member index to lock queue
        lockWaiters.push(memberIdx);
    }
}

void DistributedLock::unlock(int32_t memberIdx)
{
    faabric::util::UniqueLock lock(mx);

    ownerIdx = -1;

    if (lockWaiters.size() > 0) {
        ownerIdx = lockWaiters.front();
        lockWaiters.pop();

        SPDLOG_TRACE(
          "Member {} unlocked {}. Notifying {}", memberIdx, groupId, ownerIdx);

        notifyLocked(ownerIdx);
    } else {
        SPDLOG_TRACE("Member {} unlocked {}", memberIdx, groupId);
    }
}

void DistributedLock::notifyLocked(int32_t memberIdx)
{
    std::vector<uint8_t> data(1, 0);

    ptpBroker.sendMessage(groupId, 0, memberIdx, data.data(), data.size());
}

DistributedBarrier::DistributedBarrier(int32_t groupIdIn, int32_t groupSizeIn)
  : groupId(groupIdIn)
  , groupSize(groupSizeIn)
  , ptpBroker(faabric::transport::getPointToPointBroker())
{}

void DistributedBarrier::wait(int32_t memberIdx)
{
    faabric::util::UniqueLock lock(mx);

    barrierWaiters.insert(memberIdx);

    if (barrierWaiters.size() == groupSize) {
        SPDLOG_TRACE("Member {} completes barrier {}. Notifying {} waiters",
                     memberIdx,
                     groupId,
                     groupSize);

        // Notify everyone we're done
        for (auto m : barrierWaiters) {
            std::vector<uint8_t> data(1, 0);

            ptpBroker.sendMessage(groupId, 0, m, data.data(), data.size());
        }
    } else {
        SPDLOG_TRACE("Member {} waiting on barrier {}. {}/{} waiters",
                     memberIdx,
                     groupId,
                     barrierWaiters.size(),
                     groupSize);
    }
}

DistributedCoordinationGroup::DistributedCoordinationGroup(int32_t groupIdIn,
                                                           int32_t groupSizeIn)
  : groupId(groupIdIn)
  , groupSize(groupSizeIn)
  , barrier(groupId, groupSize)
  , lock(groupId, false)
  , recursiveLock(groupId, true)
  , notify(groupId, groupSize)
{}

DistributedCoordinationGroup& DistributedCoordinator::getCoordinationGroup(
  int32_t groupId)
{
    // Should not be calling this with a zero group ID
    checkGroupIdSet(groupId);

    if (groups.find(groupId) == groups.end()) {
        SPDLOG_ERROR("Did not find group ID {} on this host", groupId);
        throw std::runtime_error("Group ID not found on host");
    }

    return groups.at(groupId);
}

DistributedCoordinationGroup&
DistributedCoordinator::getOrCreateCoordinationGroup(int32_t groupId,
                                                     int32_t groupSize)
{
    checkGroupIdSet(groupId);

    if (groups.find(groupId) == groups.end()) {
        faabric::util::FullLock lock(sharedMutex);
        if (groups.find(groupId) == groups.end()) {
            groups.emplace(groupId, groupSize);
        }
    }

    {
        faabric::util::SharedLock lock(sharedMutex);
        return groups.at(groupId);
    }
}

DistributedCoordinator& getDistributedCoordinator()
{
    static DistributedCoordinator sync;
    return sync;
}

faabric::scheduler::FunctionCallClient& getFunctionCallClient(
  const faabric::Message& msg)
{
    return faabric::scheduler::getScheduler().getFunctionCallClient(
      msg.masterhost());
}

DistributedCoordinator::DistributedCoordinator()
  : conf(faabric::util::getSystemConfig())
{}

void DistributedCoordinator::clear()
{
    groups.clear();
}

void DistributedCoordinator::initGroup(int32_t groupId, int32_t groupSize)
{
    checkGroupIdSet(groupId);

    // This will implicitly initialise the group
    getOrCreateCoordinationGroup(groupId, groupSize);
}

// -----------------------------
// LOCK
// -----------------------------

void DistributedCoordinator::lock(const faabric::Message& msg)
{
    checkGroupIdSet(msg.groupid());

    int32_t groupId = msg.groupid();
    if (msg.masterhost() == conf.endpointHost) {
        localLock(groupId, msg.groupsize());
    } else {
        getFunctionCallClient(msg).coordinationLock(msg);
    }
}

void DistributedCoordinator::localLock(const faabric::Message& msg)
{
    localLock(msg.groupid(), msg.groupsize());
}

void DistributedCoordinator::localLock(int32_t groupId, int32_t groupMember)
{
    LOCK_TIMEOUT(getCoordinationGroup(groupId).mutex, timeoutMs);
}

void DistributedCoordinator::localLock(int32_t groupId,
                                       int32_t groupSize,
                                       int32_t groupMember)
{
    LOCK_TIMEOUT(getOrCreateCoordinationGroup(groupId, groupSize).mutex,
                 timeoutMs);
}

// -----------------------------
// UNLOCK
// -----------------------------

void DistributedCoordinator::unlock(const faabric::Message& msg)
{
    checkGroupIdSet(msg.groupid());

    int32_t groupId = msg.groupid();
    if (msg.masterhost() == conf.endpointHost) {
        localUnlock(groupId, msg.groupsize());
    } else {
        getFunctionCallClient(msg).coordinationUnlock(msg);
    }
}

void DistributedCoordinator::localUnlock(const faabric::Message& msg)
{
    localUnlock(msg.groupid(), msg.groupsize());
}

void DistributedCoordinator::localUnlock(int32_t groupId, int32_t groupMember)
{
    getCoordinationGroup(groupId).mutex.unlock();
}

void DistributedCoordinator::localUnlock(int32_t groupId,
                                         int32_t groupSize,
                                         int32_t groupMember)
{
    getOrCreateCoordinationGroup(groupId, groupSize).mutex.unlock();
}

// -----------------------------
// TRY LOCK
// -----------------------------

bool DistributedCoordinator::localTryLock(const faabric::Message& msg)
{
    return localTryLock(msg.groupid(), msg.groupsize());
}

bool DistributedCoordinator::localTryLock(int32_t groupId,
                                          int32_t groupSize,
                                          int32_t groupMember)
{
    return getOrCreateCoordinationGroup(groupId, groupSize).mutex.try_lock();
}

// -----------------------------
// RECURSIVE LOCK/ UNLOCK
// -----------------------------

void DistributedCoordinator::localLockRecursive(const faabric::Message& msg)
{
    localLockRecursive(msg.groupid(), msg.groupsize());
}

void DistributedCoordinator::localLockRecursive(int32_t groupId,
                                                int32_t groupSize,
                                                int32_t groupMember)
{
    LOCK_TIMEOUT(
      getOrCreateCoordinationGroup(groupId, groupSize).recursiveMutex,
      timeoutMs);
}

void DistributedCoordinator::localUnlockRecursive(const faabric::Message& msg)
{
    localUnlockRecursive(msg.groupid(), msg.groupsize());
}

void DistributedCoordinator::localUnlockRecursive(int32_t groupId,
                                                  int32_t groupSize)
{
    getOrCreateCoordinationGroup(groupId, groupSize).recursiveMutex.unlock();
}

// -----------------------------
// NOTIFY
// -----------------------------

void DistributedCoordinator::notify(const faabric::Message& msg)
{
    checkGroupIdSet(msg.groupid());

    int32_t groupId = msg.groupid();
    if (msg.masterhost() == conf.endpointHost) {
        localNotify(groupId, msg.groupsize(), msg.appindex());
    } else {
        getFunctionCallClient(msg).coordinationNotify(msg);
    }
}

void DistributedCoordinator::localNotify(const faabric::Message& msg)
{
    localNotify(msg.groupid(), msg.groupsize(), msg.appindex());
}

void DistributedCoordinator::localNotify(int32_t groupId,
                                         int32_t groupSize,
                                         int32_t groupMember)
{
    doLocalNotify(groupId, groupSize, false);
}

void DistributedCoordinator::awaitNotify(const faabric::Message& msg)
{
    awaitNotify(msg.groupid(), msg.groupsize());
}

void DistributedCoordinator::awaitNotify(int32_t groupId,
                                         int32_t groupSize,
                                         int32_t groupMember)
{
    doLocalNotify(groupId, groupSize, true);
}

void DistributedCoordinator::doLocalNotify(int32_t groupId,
                                           int32_t groupSize,
                                           bool master)
{
    checkGroupSizeSet(groupSize);

    DistributedCoordinationGroup& group =
      getOrCreateCoordinationGroup(groupId, groupSize);

    // All members must lock when entering this function
    std::unique_lock<std::mutex> lock(group.notifyMutex);

    if (master) {
        auto timePoint = std::chrono::system_clock::now() +
                         std::chrono::milliseconds(GROUP_TIMEOUT_MS);

        if (!group.cv.wait_until(lock, timePoint, [&] {
                return group.count.load() >= groupSize - 1;
            })) {

            SPDLOG_ERROR("Group {} await notify timed out", groupId);
            throw std::runtime_error("Group notify timed out");
        }

        // Reset, after we've finished
        group.count.store(0);
    } else {
        // If this is the last non-master member, notify
        int countBefore = group.count.fetch_add(1);
        if (countBefore == groupSize - 2) {
            group.cv.notify_one();
        } else if (countBefore > groupSize - 2) {
            SPDLOG_ERROR("Group {} notify exceeded group size, {} > {}",
                         groupId,
                         countBefore,
                         groupSize - 2);
            throw std::runtime_error("Group notify exceeded size");
        }
    }
}

// -----------------------------
// BARRIER
// -----------------------------

void DistributedCoordinator::barrier(const faabric::Message& msg)
{
    checkGroupIdSet(msg.groupid());
    int32_t groupId = msg.groupid();
    if (msg.masterhost() == conf.endpointHost) {
        localBarrier(groupId, msg.groupsize());
    } else {
        getFunctionCallClient(msg).coordinationBarrier(msg);
    }
}

void DistributedCoordinator::localBarrier(const faabric::Message& msg)
{
    localBarrier(msg.groupid(), msg.groupsize());
}

void DistributedCoordinator::localBarrier(int32_t groupId, int32_t groupSize)
{
    checkGroupSizeSet(groupSize);

    DistributedCoordinationGroup& group =
      getOrCreateCoordinationGroup(groupId, groupSize);

    group.barrier->wait();
}

// -----------------------------
// STATE
// -----------------------------

bool DistributedCoordinator::isLocalLocked(const faabric::Message& msg)
{
    DistributedCoordinationGroup& group =
      getOrCreateCoordinationGroup(msg.groupid(), msg.groupsize());
    bool canLock = group.mutex.try_lock();

    if (canLock) {
        group.mutex.unlock();
        return false;
    }

    return true;
}

int32_t DistributedCoordinator::getNotifyCount(const faabric::Message& msg)
{
    DistributedCoordinationGroup& group =
      getOrCreateCoordinationGroup(msg.groupid(), msg.groupsize());
    std::unique_lock<std::mutex> lock(group.notifyMutex);

    return group.count.load();
}

void DistributedCoordinator::checkGroupIdSet(int32_t groupId)
{
    if (groupId <= 0) {
        throw std::runtime_error("Message does not have group id set");
    }
}

void DistributedCoordinator::checkGroupSizeSet(int32_t groupSize)
{
    if (groupSize <= 0) {
        throw std::runtime_error("Message does not have group size set");
    }
}
}
