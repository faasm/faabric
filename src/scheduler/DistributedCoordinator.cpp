#include <faabric/scheduler/DistributedCoordinator.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/barrier.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/testing.h>
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

// -----------------------------------
// COORDINATION GROUP
// -----------------------------------

DistributedCoordinationGroup::DistributedCoordinationGroup(
  const std::string& masterHostIn,
  int32_t groupIdIn,
  int32_t groupSizeIn)
  : masterHost(masterHostIn)
  , groupId(groupIdIn)
  , groupSize(groupSizeIn)
  , isMasteredThisHost(faabric::util::getSystemConfig().endpointHost ==
                       masterHost)
  , masterClient(masterHost)
  , ptpBroker(faabric::transport::getPointToPointBroker())
{}

void DistributedCoordinationGroup::lock(int32_t groupIdx, bool recursive)
{
    if (!isMasteredThisHost) {
        // Send remote request
        masterClient.coordinationLock(groupId, groupIdx, recursive);

        // Await ptp response
        ptpBroker.recvMessage(groupId, 0, groupIdx);

        return;
    }

    bool success = false;
    {
        faabric::util::UniqueLock lock(mx);
        if (recursive) {
            bool isFree = recursiveLockOwners.empty();

            bool isLockedByMe =
              !isFree && (recursiveLockOwners.top() == groupIdx);

            if (isFree || isLockedByMe) {
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
        } else if (!recursive && lockOwnerIdx == -1) {
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

void DistributedCoordinationGroup::localLock(bool recursive)
{
    if (recursive) {
        LOCK_TIMEOUT(localRecursiveMx, timeoutMs);
    } else {
        LOCK_TIMEOUT(localMx, timeoutMs);
    }
}

bool DistributedCoordinationGroup::localTryLock()
{
    SPDLOG_TRACE("Trying local lock on {}", groupId);
    return localMx.try_lock();
}

void DistributedCoordinationGroup::unlock(int32_t groupIdx, bool recursive)
{
    if (!isMasteredThisHost) {
        // Send remote request
        masterClient.coordinationUnlock(groupId, groupIdx, recursive);
        return;
    }

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
        lockOwnerIdx = -1;

        if (!lockWaiters.empty()) {
            lockOwnerIdx = lockWaiters.front();
            notifyLocked(lockWaiters.front());
            lockWaiters.pop();
        }
    }
}

void DistributedCoordinationGroup::localUnlock(bool recursive)
{
    if (recursive) {
        localRecursiveMx.unlock();
    } else {
        localMx.unlock();
    }
}

void DistributedCoordinationGroup::notifyLocked(int32_t groupIdx)
{
    std::vector<uint8_t> data(1, 0);

    ptpBroker.sendMessage(groupId, 0, groupIdx, data.data(), data.size());
}

void DistributedCoordinationGroup::barrier(int32_t groupIdx)
{
    // TODO implement a more efficient barrier implementation to avoid load on
    // the master
    if (groupIdx == 0) {
        // Receive from all
        for (int i = 1; i < groupSize; i++) {
            ptpBroker.recvMessage(groupId, i, 0);
        }

        // Reply to all
        std::vector<uint8_t> data(1, 0);
        for (int i = 1; i < groupSize; i++) {
            ptpBroker.sendMessage(groupId, 0, i, data.data(), data.size());
        }
    } else {
        // Do the send
        std::vector<uint8_t> data(1, 0);
        ptpBroker.sendMessage(groupId, groupIdx, 0, data.data(), data.size());
    }
}

void DistributedCoordinationGroup::notify(int32_t groupIdx)
{
    if (groupIdx == 0) {
        for (int i = 1; i < groupSize; i++) {
            ptpBroker.recvMessage(groupId, i, 0);
        }
    } else {
        std::vector<uint8_t> data(1, 0);
        ptpBroker.sendMessage(groupId, groupIdx, 0, data.data(), data.size());
    }
}

bool DistributedCoordinationGroup::isLocalLockable(bool recursive)
{
    if (recursive) {
        bool canLock = localRecursiveMx.try_lock();
        if (canLock) {
            localRecursiveMx.unlock();
            return true;
        }
    } else {
        bool canLock = localMx.try_lock();
        if (canLock) {
            localMx.unlock();
            return true;
        }
    }
    return false;
}

void DistributedCoordinationGroup::overrideMasterHost(const std::string& host)
{
    masterHost = host;
    isMasteredThisHost = faabric::util::getSystemConfig().endpointHost == host;
}

// -----------------------------------
// COORDINATOR
// -----------------------------------

DistributedCoordinator::DistributedCoordinator()
  : conf(faabric::util::getSystemConfig())
{}

std::shared_ptr<DistributedCoordinationGroup>
DistributedCoordinator::getCoordinationGroup(int32_t groupId)
{
    if (groups.find(groupId) == groups.end()) {
        SPDLOG_ERROR("Did not find group ID {} on this host", groupId);
        throw std::runtime_error("Group ID not found on host");
    }

    return groups.at(groupId);
}

std::shared_ptr<DistributedCoordinationGroup> DistributedCoordinator::initGroup(
  const std::string& masterHost,
  int32_t groupId,
  int32_t groupSize)
{
    if (groups.find(groupId) == groups.end()) {
        faabric::util::FullLock lock(sharedMutex);
        if (groups.find(groupId) == groups.end()) {
            groups.emplace(
              std::make_pair(groupId,
                             std::make_shared<DistributedCoordinationGroup>(
                               masterHost, groupId, groupSize)));
        }
    }

    {
        faabric::util::SharedLock lock(sharedMutex);
        return groups.at(groupId);
    }
}

std::shared_ptr<DistributedCoordinationGroup> DistributedCoordinator::initGroup(
  const faabric::Message& msg)
{
    return initGroup(msg.masterhost(), msg.groupid(), msg.groupsize());
}

DistributedCoordinator& getDistributedCoordinator()
{
    static DistributedCoordinator sync;
    return sync;
}

void DistributedCoordinator::clear()
{
    groups.clear();
}

bool DistributedCoordinator::groupExists(int32_t groupId)
{
    faabric::util::SharedLock lock(sharedMutex);
    return groups.find(groupId) != groups.end();
}
}
