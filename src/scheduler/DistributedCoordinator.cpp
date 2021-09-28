#include "faabric/util/barrier.h"
#include <faabric/scheduler/DistributedCoordinator.h>
#include <faabric/util/timing.h>

#define GROUP_TIMEOUT_MS 20000

#define DISTRIBUTED_SYNC_OP(opLocal, opRemote)                                 \
    {                                                                          \
        int32_t groupId = msg.groupid();                                       \
        if (msg.masterhost() == sch.getThisHost()) {                           \
            opLocal(groupId, msg.groupsize());                                 \
        } else {                                                               \
            std::string host = msg.masterhost();                               \
            faabric::scheduler::FunctionCallClient& client =                   \
              sch.getFunctionCallClient(host);                                 \
            opRemote(msg);                                                     \
        }                                                                      \
    }

namespace faabric::scheduler {

DistributedCoordinationGroup::DistributedCoordinationGroup(int32_t groupSizeIn)
  : groupSize(groupSizeIn)
  , barrier(faabric::util::Barrier::create(groupSizeIn))
{}

DistributedCoordinationGroup& DistributedCoordinator::getCoordinationGroup(
  int32_t groupId,
  int32_t groupSize)
{
    {
        if (groups.find(groupId) == groups.end()) {
            faabric::util::FullLock lock(sharedMutex);
            if (groups.find(groupId) == groups.end()) {
                groups.emplace(groupId, groupSize);
            }
        }
    }

    {
        faabric::util::SharedLock lock(sharedMutex);
        return groups[groupId];
    }
}

DistributedCoordinator& getDistributedCoordinator()
{
    static DistributedCoordinator sync;
    return sync;
}

DistributedCoordinator::DistributedCoordinator()
  : sch(faabric::scheduler::getScheduler())
{}

void DistributedCoordinator::clear()
{
    groups.clear();
}

void DistributedCoordinator::localLockRecursive(int32_t groupId,
                                                int32_t groupSize)
{
    getCoordinationGroup(groupId, groupSize).recursiveMutex.lock();
}

void DistributedCoordinator::localLock(int32_t groupId, int32_t groupSize)
{
    getCoordinationGroup(groupId, groupSize).mutex.lock();
}

bool DistributedCoordinator::localTryLock(int32_t groupId, int32_t groupSize)
{
    getCoordinationGroup(groupId, groupSize).mutex.try_lock();
}

void DistributedCoordinator::lock(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localLock, client.coordinationLock)
}

void DistributedCoordinator::localUnlockRecursive(int32_t groupId)
{
    FROM_MAP(mx, std::recursive_mutex, recursiveMutexes);
    mx->unlock();
}

void DistributedCoordinator::localUnlock(int32_t groupId)
{
    FROM_MAP(mx, std::mutex, mutexes);
    mx->unlock();
}

void DistributedCoordinator::unlock(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localUnlock, client.coordinationUnlock)
}

void DistributedCoordinator::doLocalNotify(int32_t groupId, bool master)
{
    checkGroupSizeSet(groupId);

    // All members must lock when entering this function
    FROM_MAP(nowaitMutex, std::mutex, mutexes)
    std::unique_lock<std::mutex> lock(*nowaitMutex);

    FROM_MAP(nowaitCount, std::atomic<int>, counts)
    FROM_MAP(nowaitCv, std::condition_variable, cvs)

    int appSize = groupSizes[groupId];

    if (master) {
        auto timePoint = std::chrono::system_clock::now() +
                         std::chrono::milliseconds(GROUP_TIMEOUT_MS);

        if (!nowaitCv->wait_until(lock, timePoint, [&] {
                return nowaitCount->load() >= appSize - 1;
            })) {

            SPDLOG_ERROR("Group {} await notify timed out", groupId);
            throw std::runtime_error("Group notify timed out");
        }

        // Reset, after we've finished
        nowaitCount->store(0);
    } else {
        // If this is the last non-master member, notify
        int countBefore = nowaitCount->fetch_add(1);
        if (countBefore == appSize - 2) {
            nowaitCv->notify_one();
        } else if (countBefore > appSize - 2) {
            SPDLOG_ERROR("Group {} notify exceeded group size, {} > {}",
                         groupId,
                         countBefore,
                         appSize - 2);
            throw std::runtime_error("Group notify exceeded size");
        }
    }
}

void DistributedCoordinator::localNotify(int32_t groupId)
{
    doLocalNotify(groupId, false);
}

void DistributedCoordinator::awaitNotify(int32_t groupId)
{
    doLocalNotify(groupId, true);
}

void DistributedCoordinator::notify(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localNotify, client.coordinationNotify)
}

void DistributedCoordinator::localBarrier(int32_t groupId)
{
    checkGroupSizeSet(groupId);
    int32_t appSize = groupSizes[groupId];

    // Create if necessary
    if (barriers.find(groupId) == barriers.end()) {
        faabric::util::FullLock lock(sharedMutex);
        if (barriers.find(groupId) == barriers.end()) {
            barriers[groupId] = faabric::util::Barrier::create(appSize);
        }
    }

    // Wait
    std::shared_ptr<faabric::util::Barrier> barrier;
    {
        faabric::util::SharedLock lock(sharedMutex);
        barrier = barriers[groupId];
    }

    barrier->wait();
}

void DistributedCoordinator::barrier(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localBarrier, client.coordinationBarrier)
}

bool DistributedCoordinator::isLocalLocked(int32_t groupId)
{
    FROM_MAP(mx, std::mutex, mutexes);

    bool canLock = mx->try_lock();

    if (canLock) {
        mx->unlock();
        return false;
    }

    return true;
}

int32_t DistributedCoordinator::getNotifyCount(int32_t groupId)
{
    FROM_MAP(nowaitMutex, std::mutex, mutexes)
    std::unique_lock<std::mutex> lock(*nowaitMutex);

    FROM_MAP(nowaitCount, std::atomic<int>, counts)

    return nowaitCount->load();
}

int32_t DistributedCoordinator::getGroupSize(int32_t groupId)
{
    checkGroupSizeSet(groupId);
    return groupSizes[groupId];
}
}
