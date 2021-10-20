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
  , localBarrier(faabric::util::Barrier::create(groupSize, timeoutMs))
{}

void DistributedCoordinationGroup::lock(int32_t groupIdx, bool recursive)
{
    if (!isMasteredThisHost) {
        // Send remote request
        masterClient.coordinationLock(groupId, groupSize, groupIdx, recursive);

        // Await ptp response
        ptpBroker.recvMessage(groupId, 0, groupIdx);

        return;
    }

    faabric::util::UniqueLock lock(mx);

    bool success = false;

    if (recursive && (recursiveLockOwners.empty() ||
                      recursiveLockOwners.top() == groupIdx)) {
        recursiveLockOwners.push(groupIdx);
        success = true;
    } else if (lockOwnerIdx == -1) {
        lockOwnerIdx = groupIdx;
        success = true;
    }

    if (success) {
        SPDLOG_TRACE("Member {} locked {}", groupIdx, groupId, groupIdx);

        notifyLocked(groupIdx);
    } else {
        SPDLOG_TRACE("Member {} waiting on lock {}. Already locked by {}",
                     groupIdx,
                     groupId,
                     groupIdx);

        // Add member index to lock queue
        lockWaiters.push(groupIdx);
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
    return localMx.try_lock();
}

void DistributedCoordinationGroup::unlock(int32_t groupIdx, bool recursive)
{
    if (!isMasteredThisHost) {
        // Send remote request
        masterClient.coordinationUnlock(
          groupId, groupSize, groupIdx, recursive);
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
    if (!isMasteredThisHost) {
        // Send remote request
        masterClient.coordinationBarrier(groupId, groupSize, groupIdx);

        // Await ptp response
        ptpBroker.recvMessage(groupId, 0, groupIdx);

        return;
    }

    faabric::util::UniqueLock lock(mx);

    barrierWaiters.insert(groupIdx);

    if (barrierWaiters.size() == groupSize) {
        SPDLOG_TRACE("Member {} completes barrier {}. Notifying {} waiters",
                     groupIdx,
                     groupId,
                     groupSize);

        // Notify everyone we're done
        for (auto m : barrierWaiters) {
            std::vector<uint8_t> data(1, 0);

            ptpBroker.sendMessage(groupId, 0, m, data.data(), data.size());
        }
    } else {
        SPDLOG_TRACE("Member {} waiting on barrier {}. {}/{} waiters",
                     groupIdx,
                     groupId,
                     barrierWaiters.size(),
                     groupSize);
    }
}

void DistributedCoordinationGroup::notify(int32_t groupIdx)
{
    if (!isMasteredThisHost) {
        // Send remote request
        masterClient.coordinationNotify(groupId, groupSize, groupIdx);
        return;
    }

    // All members must lock when entering this function
    std::unique_lock<std::mutex> lock(notifyMutex);

    if (groupIdx == 0) {
        auto timePoint = std::chrono::system_clock::now() +
                         std::chrono::milliseconds(GROUP_TIMEOUT_MS);

        if (!notifyCv.wait_until(
              lock, timePoint, [&] { return notifyCount >= groupSize - 1; })) {

            SPDLOG_ERROR("Group {} await notify timed out", groupId);
            throw std::runtime_error("Group notify timed out");
        }

        // Reset, after we've finished
        notifyCount = 0;
    } else {
        // If this is the last non-master member, notify
        notifyCount += 1;
        if (notifyCount == groupSize - 1) {
            notifyCv.notify_one();
        } else if (notifyCount > groupSize - 1) {
            SPDLOG_ERROR("Group {} notify exceeded group size, {} > {}",
                         groupId,
                         notifyCount,
                         groupSize - 1);
            throw std::runtime_error("Group notify exceeded size");
        }
    }
}

bool DistributedCoordinationGroup::isLocalLocked()
{
    bool canLock = localMx.try_lock();
    if (canLock) {
        localMx.unlock();
        return false;
    }

    return true;
}

int32_t DistributedCoordinationGroup::getNotifyCount()
{
    return notifyCount;
}

void DistributedCoordinationGroup::overrideMasterHost(const std::string& host)
{
    masterHost = host;
    isMasteredThisHost = faabric::util::getSystemConfig().endpointHost == host;
}

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

DistributedCoordinator::DistributedCoordinator()
  : conf(faabric::util::getSystemConfig())
{}

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
