#include <faabric/scheduler/DistributedSync.h>
#include <faabric/util/timing.h>

#define GROUP_TIMEOUT_MS 20000

#define DISTRIBUTED_SYNC_OP(opLocal, opRemote)                                 \
    {                                                                          \
        int32_t groupId = msg.appid();                                         \
        if (msg.masterhost() == sch.getThisHost()) {                           \
            opLocal(groupId);                                                  \
        } else {                                                               \
            std::string host = msg.masterhost();                               \
            faabric::scheduler::FunctionCallClient& client =                   \
              sch.getFunctionCallClient(host);                                 \
            opRemote(groupId);                                                 \
        }                                                                      \
    }

#define FROM_MAP(varName, T, m, ...)                                           \
    {                                                                          \
        if (m.find(groupId) == m.end()) {                                      \
            faabric::util::FullLock lock(sharedMutex);                         \
            if (m.find(groupId) == m.end()) {                                  \
                m[groupId] = std::make_shared<T>(__VA_ARGS__);                 \
            }                                                                  \
        }                                                                      \
    }                                                                          \
    std::shared_ptr<T> varName;                                                \
    {                                                                          \
        faabric::util::SharedLock lock(sharedMutex);                           \
        varName = m[groupId];                                                  \
    }

namespace faabric::scheduler {

DistributedSync& getDistributedSync()
{
    static DistributedSync sync;
    return sync;
}

DistributedSync::DistributedSync()
  : sch(faabric::scheduler::getScheduler())
{}

void DistributedSync::setGroupSize(const faabric::Message& msg, int groupSize)
{
    if (msg.masterhost() != sch.getThisHost()) {
        SPDLOG_ERROR("Setting group {} size not on master ({} != {})",
                     msg.appid(),
                     msg.masterhost(),
                     sch.getThisHost());

        throw std::runtime_error("Setting sync group size on non-master");
    }

    groupSizes[msg.appid()] = groupSize;
}

void DistributedSync::checkGroupSizeSet(int32_t groupId)
{
    if (groupSizes.find(groupId) == groupSizes.end()) {
        SPDLOG_ERROR("Group {} size not set", groupId);
        throw std::runtime_error("Group size not set");
    }
}

void DistributedSync::clear()
{
    groupSizes.clear();

    barriers.clear();

    recursiveMutexes.clear();
    mutexes.clear();

    counts.clear();
    cvs.clear();
}

void DistributedSync::localLockRecursive(int32_t groupId)
{
    FROM_MAP(mx, std::recursive_mutex, recursiveMutexes);
    mx->lock();
}

void DistributedSync::localLock(int32_t groupId)
{
    FROM_MAP(mx, std::mutex, mutexes);
    mx->lock();
}

bool DistributedSync::localTryLock(int32_t groupId)
{
    FROM_MAP(mx, std::mutex, mutexes);
    return mx->try_lock();
}

void DistributedSync::lock(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localLock, client.functionGroupLock)
}

void DistributedSync::localUnlockRecursive(int32_t groupId)
{
    FROM_MAP(mx, std::recursive_mutex, recursiveMutexes);
    mx->unlock();
}

void DistributedSync::localUnlock(int32_t groupId)
{
    FROM_MAP(mx, std::mutex, mutexes);
    mx->unlock();
}

void DistributedSync::unlock(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localUnlock, client.functionGroupUnlock)
}

void DistributedSync::doLocalNotify(int32_t groupId, bool master)
{
    checkGroupSizeSet(groupId);

    // All members must lock when entering this function
    FROM_MAP(nowaitMutex, std::mutex, mutexes)
    std::unique_lock<std::mutex> lock(*nowaitMutex);

    FROM_MAP(nowaitCount, std::atomic<int>, counts)
    FROM_MAP(nowaitCv, std::condition_variable, cvs)

    int groupSize = groupSizes[groupId];

    if (master) {
        auto timePoint = std::chrono::system_clock::now() +
                         std::chrono::milliseconds(GROUP_TIMEOUT_MS);

        if (!nowaitCv->wait_until(lock, timePoint, [&] {
                return nowaitCount->load() >= groupSize - 1;
            })) {

            SPDLOG_ERROR("Group {} await notify timed out", groupId);
            throw std::runtime_error("Group notify timed out");
        }

        // Reset, after we've finished
        nowaitCount->store(0);
    } else {
        // If this is the last non-master member, notify
        int countBefore = nowaitCount->fetch_add(1);
        if (countBefore == groupSize - 2) {
            nowaitCv->notify_one();
        } else if (countBefore > groupSize - 2) {
            SPDLOG_ERROR("Group {} notify exceeded group size, {} > {}",
                         groupId,
                         countBefore,
                         groupSize - 2);
            throw std::runtime_error("Group notify exceeded size");
        }
    }
}

void DistributedSync::localNotify(int32_t groupId)
{
    doLocalNotify(groupId, false);
}

void DistributedSync::awaitNotify(int32_t groupId)
{
    doLocalNotify(groupId, true);
}

void DistributedSync::notify(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localNotify, client.functionGroupNotify)
}

void DistributedSync::localBarrier(int32_t groupId)
{
    checkGroupSizeSet(groupId);
    int32_t groupSize = groupSizes[groupId];

    // Create if necessary
    if (barriers.find(groupId) == barriers.end()) {
        faabric::util::FullLock lock(sharedMutex);
        if (barriers.find(groupId) == barriers.end()) {
            barriers[groupId] = faabric::util::Barrier::create(groupSize);
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

void DistributedSync::barrier(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localBarrier, client.functionGroupBarrier)
}

bool DistributedSync::isLocalLocked(int32_t groupId)
{
    FROM_MAP(mx, std::mutex, mutexes);

    bool canLock = mx->try_lock();

    if (canLock) {
        mx->unlock();
        return false;
    }

    return true;
}

int32_t DistributedSync::getNotifyCount(int32_t groupId)
{
    FROM_MAP(nowaitMutex, std::mutex, mutexes)
    std::unique_lock<std::mutex> lock(*nowaitMutex);

    FROM_MAP(nowaitCount, std::atomic<int>, counts)

    return nowaitCount->load();
}

int32_t DistributedSync::getGroupSize(int32_t groupId)
{
    checkGroupSizeSet(groupId);
    return groupSizes[groupId];
}
}
