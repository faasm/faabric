#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/barrier.h>
#include <faabric/util/locks.h>

namespace faabric::scheduler {

class DistributedCoordination
{
  public:
    DistributedCoordination();

    void setAppSize(const faabric::Message& msg, int appSize);

    void clear();

    // --- Lock ---
    void lock(const faabric::Message& msg);

    void localLock(int32_t appId);

    // --- Unlock ---
    void unlock(const faabric::Message& msg);

    void localUnlock(int32_t appId);

    // --- Try lock ---
    bool localTryLock(int32_t appId);

    // --- Lock recursive ---
    void localLockRecursive(int32_t appId);

    // --- Unlock recursive
    void localUnlockRecursive(int32_t appId);

    // --- Notify ---
    void notify(const faabric::Message& msg);

    void localNotify(int32_t appId);

    void awaitNotify(int32_t appId);

    // --- Barrier ---
    void localBarrier(int32_t appId);

    void barrier(const faabric::Message& msg);

    // --- Querying state ---
    bool isLocalLocked(int32_t appId);

    int32_t getNotifyCount(int32_t appId);

    int32_t getAppSize(int32_t appId);

  private:
    faabric::scheduler::Scheduler& sch;

    std::shared_mutex sharedMutex;

    std::unordered_map<int32_t, int32_t> appSizes;

    std::unordered_map<uint32_t, std::shared_ptr<faabric::util::Barrier>>
      barriers;

    std::unordered_map<uint32_t, std::shared_ptr<std::recursive_mutex>>
      recursiveMutexes;

    std::unordered_map<uint32_t, std::shared_ptr<std::mutex>> mutexes;

    std::unordered_map<uint32_t, std::shared_ptr<std::atomic<int>>> counts;
    std::unordered_map<uint32_t, std::shared_ptr<std::condition_variable>> cvs;

    void doLocalNotify(int32_t appId, bool master);

    void checkAppSizeSet(int32_t appId);
};

DistributedCoordination& getDistributedCoordination();
}
