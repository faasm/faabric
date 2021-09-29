#include <faabric/scheduler/DistributedCoordinator.h>
#include <faabric/util/barrier.h>
#include <faabric/util/func.h>
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
  int32_t groupId)
{
    // Should not be calling this with a zero group ID
    assert(groupId > 0);

    if (groups.find(groupId) == groups.end()) {
        SPDLOG_ERROR("Did not find group ID {} on this host", groupId);
        throw std::runtime_error("Group ID not found on host");
    }

    return groups.at(groupId);
}

DistributedCoordinationGroup& DistributedCoordinator::getCoordinationGroup(
  int32_t groupId,
  int32_t groupSize)
{
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

DistributedCoordinator::DistributedCoordinator()
  : sch(faabric::scheduler::getScheduler())
{}

void DistributedCoordinator::clear()
{
    groups.clear();
}

void DistributedCoordinator::initGroup(int32_t groupId, int32_t groupSize)
{
    // This will implicitly initialise the group
    getCoordinationGroup(groupId, groupSize);
}

// -----------------------------
// LOCK
// -----------------------------

void DistributedCoordinator::lock(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localLock, client.coordinationLock)
}

void DistributedCoordinator::localLock(const faabric::Message& msg)
{
    localLock(msg.groupid(), msg.groupsize());
}

void DistributedCoordinator::localLock(int32_t groupId)
{
    getCoordinationGroup(groupId).mutex.lock();
}

void DistributedCoordinator::localLock(int32_t groupId, int32_t groupSize)
{
    getCoordinationGroup(groupId, groupSize).mutex.lock();
}

// -----------------------------
// UNLOCK
// -----------------------------

void DistributedCoordinator::unlock(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localUnlock, client.coordinationUnlock)
}

void DistributedCoordinator::localUnlock(const faabric::Message& msg)
{
    localUnlock(msg.groupid(), msg.groupsize());
}

void DistributedCoordinator::localUnlock(int32_t groupId)
{
    getCoordinationGroup(groupId).mutex.unlock();
}

void DistributedCoordinator::localUnlock(int32_t groupId, int32_t groupSize)
{
    getCoordinationGroup(groupId, groupSize).mutex.unlock();
}

// -----------------------------
// TRY LOCK
// -----------------------------

bool DistributedCoordinator::localTryLock(const faabric::Message& msg)
{
    return localTryLock(msg.groupid(), msg.groupsize());
}

bool DistributedCoordinator::localTryLock(int32_t groupId, int32_t groupSize)
{
    return getCoordinationGroup(groupId, groupSize).mutex.try_lock();
}

// -----------------------------
// RECURSIVE LOCK/ UNLOCK
// -----------------------------

void DistributedCoordinator::localLockRecursive(const faabric::Message& msg)
{
    localLockRecursive(msg.groupid(), msg.groupsize());
}

void DistributedCoordinator::localLockRecursive(int32_t groupId,
                                                int32_t groupSize)
{
    getCoordinationGroup(groupId, groupSize).recursiveMutex.lock();
}

void DistributedCoordinator::localUnlockRecursive(const faabric::Message& msg)
{
    localUnlockRecursive(msg.groupid(), msg.groupsize());
}

void DistributedCoordinator::localUnlockRecursive(int32_t groupId,
                                                  int32_t groupSize)
{
    getCoordinationGroup(groupId, groupSize).recursiveMutex.unlock();
}

// -----------------------------
// NOTIFY
// -----------------------------

void DistributedCoordinator::notify(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localNotify, client.coordinationNotify)
}

void DistributedCoordinator::localNotify(const faabric::Message& msg)
{
    localNotify(msg.groupid(), msg.groupsize());
}

void DistributedCoordinator::localNotify(int32_t groupId, int32_t groupSize)
{
    doLocalNotify(groupId, groupSize, false);
}

void DistributedCoordinator::awaitNotify(const faabric::Message& msg)
{
    awaitNotify(msg.groupid(), msg.groupsize());
}

void DistributedCoordinator::awaitNotify(int32_t groupId, int32_t groupSize)
{
    doLocalNotify(groupId, groupSize, true);
}

void DistributedCoordinator::doLocalNotify(int32_t groupId,
                                           int32_t groupSize,
                                           bool master)
{
    checkGroupSizeSet(groupId);

    DistributedCoordinationGroup& group =
      getCoordinationGroup(groupId, groupSize);

    // All members must lock when entering this function
    std::unique_lock<std::mutex> lock(group.mutex);

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
    DISTRIBUTED_SYNC_OP(localBarrier, client.coordinationBarrier)
}

void DistributedCoordinator::localBarrier(const faabric::Message& msg)
{
    localBarrier(msg.groupid(), msg.groupsize());
}

void DistributedCoordinator::localBarrier(int32_t groupId, int32_t groupSize)
{
    checkGroupSizeSet(groupSize);

    DistributedCoordinationGroup& group =
      getCoordinationGroup(groupId, groupSize);

    group.barrier->wait();
}

// -----------------------------
// STATE
// -----------------------------

bool DistributedCoordinator::isLocalLocked(const faabric::Message& msg)
{
    DistributedCoordinationGroup& group =
      getCoordinationGroup(msg.groupid(), msg.groupsize());
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
      getCoordinationGroup(msg.groupid(), msg.groupsize());
    std::unique_lock<std::mutex> lock(group.mutex);

    return group.count.load();
}

void DistributedCoordinator::checkGroupSizeSet(int32_t groupSize)
{
    if (groupSize <= 0) {
        throw std::runtime_error("Message does not have group size set");
    }
}
}
