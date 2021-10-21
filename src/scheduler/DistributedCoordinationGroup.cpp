#include <faabric/scheduler/DistributedCoordinationGroup.h>
#include <faabric/transport/PointToPointBroker.h>

#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#define NO_LOCK_OWNER_IDX -1

#define LOCK_TIMEOUT(mx, ms)                                                   \
    auto timePoint =                                                           \
      std::chrono::system_clock::now() + std::chrono::milliseconds(ms);        \
    bool success = mx.try_lock_until(timePoint);                               \
    if (!success) {                                                            \
        throw std::runtime_error("Distributed coordination timeout");          \
    }

namespace faabric::scheduler {

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

void DistributedCoordinationGroup::localLock()
{
    LOCK_TIMEOUT(localMx, timeoutMs);
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
        lockOwnerIdx = NO_LOCK_OWNER_IDX;

        if (!lockWaiters.empty()) {
            lockOwnerIdx = lockWaiters.front();
            notifyLocked(lockWaiters.front());
            lockWaiters.pop();
        }
    }
}

void DistributedCoordinationGroup::localUnlock()
{
    localMx.unlock();
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

int32_t DistributedCoordinationGroup::getLockOwner(bool recursive)
{
    if (!isMasteredThisHost) {
        SPDLOG_ERROR(
          "Cannot check if group {} locked, not mastered on this host",
          groupId);
        throw std::runtime_error("Checking group lock on non-master host");
    }

    if (recursive) {
        if (!recursiveLockOwners.empty()) {
            return recursiveLockOwners.top();
        }

        return NO_LOCK_OWNER_IDX;
    } else {
        return lockOwnerIdx;
    }
}

void DistributedCoordinationGroup::overrideMasterHost(const std::string& host)
{
    masterHost = host;
    isMasteredThisHost = faabric::util::getSystemConfig().endpointHost == host;
}
}
