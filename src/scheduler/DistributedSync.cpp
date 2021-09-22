#include <faabric/scheduler/DistributedSync.h>
#include <faabric/util/timing.h>

#define GROUP_TIMEOUT_MS 20000

#define DISTRIBUTED_SYNC_OP(opLocal, opRemote)                                 \
    {                                                                          \
        int32_t appId = msg.appid();                                         \
        if (msg.masterhost() == sch.getThisHost()) {                           \
            opLocal(appId);                                                  \
        } else {                                                               \
            std::string host = msg.masterhost();                               \
            faabric::scheduler::FunctionCallClient& client =                   \
              sch.getFunctionCallClient(host);                                 \
            opRemote(appId);                                                 \
        }                                                                      \
    }

#define FROM_MAP(varName, T, m, ...)                                           \
    {                                                                          \
        if (m.find(appId) == m.end()) {                                      \
            faabric::util::FullLock lock(sharedMutex);                         \
            if (m.find(appId) == m.end()) {                                  \
                m[appId] = std::make_shared<T>(__VA_ARGS__);                 \
            }                                                                  \
        }                                                                      \
    }                                                                          \
    std::shared_ptr<T> varName;                                                \
    {                                                                          \
        faabric::util::SharedLock lock(sharedMutex);                           \
        varName = m[appId];                                                  \
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

void DistributedSync::setAppSize(const faabric::Message& msg, int appSize)
{
    if (msg.masterhost() != sch.getThisHost()) {
        SPDLOG_ERROR("Setting group {} size not on master ({} != {})",
                     msg.appid(),
                     msg.masterhost(),
                     sch.getThisHost());

        throw std::runtime_error("Setting sync group size on non-master");
    }

    appSizes[msg.appid()] = appSize;
}

void DistributedSync::checkAppSizeSet(int32_t appId)
{
    if (appSizes.find(appId) == appSizes.end()) {
        SPDLOG_ERROR("Group {} size not set", appId);
        throw std::runtime_error("Group size not set");
    }
}

void DistributedSync::clear()
{
    appSizes.clear();

    barriers.clear();

    recursiveMutexes.clear();
    mutexes.clear();

    counts.clear();
    cvs.clear();
}

void DistributedSync::localLockRecursive(int32_t appId)
{
    FROM_MAP(mx, std::recursive_mutex, recursiveMutexes);
    mx->lock();
}

void DistributedSync::localLock(int32_t appId)
{
    FROM_MAP(mx, std::mutex, mutexes);
    mx->lock();
}

bool DistributedSync::localTryLock(int32_t appId)
{
    FROM_MAP(mx, std::mutex, mutexes);
    return mx->try_lock();
}

void DistributedSync::lock(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localLock, client.functionGroupLock)
}

void DistributedSync::localUnlockRecursive(int32_t appId)
{
    FROM_MAP(mx, std::recursive_mutex, recursiveMutexes);
    mx->unlock();
}

void DistributedSync::localUnlock(int32_t appId)
{
    FROM_MAP(mx, std::mutex, mutexes);
    mx->unlock();
}

void DistributedSync::unlock(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localUnlock, client.functionGroupUnlock)
}

void DistributedSync::doLocalNotify(int32_t appId, bool master)
{
    checkAppSizeSet(appId);

    // All members must lock when entering this function
    FROM_MAP(nowaitMutex, std::mutex, mutexes)
    std::unique_lock<std::mutex> lock(*nowaitMutex);

    FROM_MAP(nowaitCount, std::atomic<int>, counts)
    FROM_MAP(nowaitCv, std::condition_variable, cvs)

    int appSize = appSizes[appId];

    if (master) {
        auto timePoint = std::chrono::system_clock::now() +
                         std::chrono::milliseconds(GROUP_TIMEOUT_MS);

        if (!nowaitCv->wait_until(lock, timePoint, [&] {
                return nowaitCount->load() >= appSize - 1;
            })) {

            SPDLOG_ERROR("Group {} await notify timed out", appId);
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
                         appId,
                         countBefore,
                         appSize - 2);
            throw std::runtime_error("Group notify exceeded size");
        }
    }
}

void DistributedSync::localNotify(int32_t appId)
{
    doLocalNotify(appId, false);
}

void DistributedSync::awaitNotify(int32_t appId)
{
    doLocalNotify(appId, true);
}

void DistributedSync::notify(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localNotify, client.functionGroupNotify)
}

void DistributedSync::localBarrier(int32_t appId)
{
    checkAppSizeSet(appId);
    int32_t appSize = appSizes[appId];

    // Create if necessary
    if (barriers.find(appId) == barriers.end()) {
        faabric::util::FullLock lock(sharedMutex);
        if (barriers.find(appId) == barriers.end()) {
            barriers[appId] = faabric::util::Barrier::create(appSize);
        }
    }

    // Wait
    std::shared_ptr<faabric::util::Barrier> barrier;
    {
        faabric::util::SharedLock lock(sharedMutex);
        barrier = barriers[appId];
    }

    barrier->wait();
}

void DistributedSync::barrier(const faabric::Message& msg)
{
    DISTRIBUTED_SYNC_OP(localBarrier, client.functionGroupBarrier)
}

bool DistributedSync::isLocalLocked(int32_t appId)
{
    FROM_MAP(mx, std::mutex, mutexes);

    bool canLock = mx->try_lock();

    if (canLock) {
        mx->unlock();
        return false;
    }

    return true;
}

int32_t DistributedSync::getNotifyCount(int32_t appId)
{
    FROM_MAP(nowaitMutex, std::mutex, mutexes)
    std::unique_lock<std::mutex> lock(*nowaitMutex);

    FROM_MAP(nowaitCount, std::atomic<int>, counts)

    return nowaitCount->load();
}

int32_t DistributedSync::getAppSize(int32_t appId)
{
    checkAppSizeSet(appId);
    return appSizes[appId];
}
}
