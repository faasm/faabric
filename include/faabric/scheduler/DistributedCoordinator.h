#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/barrier.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>

#define DEFAULT_DISTRIBUTED_TIMEOUT_MS 30000

namespace faabric::scheduler {

class DistributedCoordinationGroup
{
  public:
    DistributedCoordinationGroup(int32_t groupSizeIn);

    int32_t groupSize;

    std::shared_ptr<faabric::util::Barrier> barrier;
    std::recursive_timed_mutex recursiveMutex;
    std::timed_mutex mutex;

    std::mutex notifyMutex;
    std::atomic<int> count = 0;
    std::condition_variable cv;
};

class DistributedCoordinator
{
  public:
    DistributedCoordinator();

    void clear();

    void initGroup(int32_t groupId, int32_t groupSize);

    // --- Lock ---
    void lock(const faabric::Message& msg);

    void localLock(const faabric::Message& msg);

    void localLock(int32_t groupId);

    void localLock(int32_t groupId, int32_t groupSize);

    // --- Unlock ---
    void unlock(const faabric::Message& msg);

    void localUnlock(const faabric::Message& msg);

    void localUnlock(int32_t groupId);

    void localUnlock(int32_t groupId, int32_t groupSize);

    // --- Try lock ---
    bool localTryLock(const faabric::Message& msg);

    bool localTryLock(int32_t groupId, int32_t groupSize);

    // --- Lock recursive ---
    void localLockRecursive(const faabric::Message& msg);

    void localLockRecursive(int32_t groupId, int32_t groupSize);

    // --- Unlock recursive
    void localUnlockRecursive(const faabric::Message& msg);

    void localUnlockRecursive(int32_t groupId, int32_t groupSize);

    // --- Notify ---
    void notify(const faabric::Message& msg);

    void localNotify(const faabric::Message& msg);

    void localNotify(int32_t groupId, int32_t groupSize);

    void awaitNotify(const faabric::Message& msg);

    void awaitNotify(int32_t groupId, int32_t groupSize);

    // --- Barrier ---
    void barrier(const faabric::Message& msg);

    void localBarrier(const faabric::Message& msg);

    void localBarrier(int32_t groupId, int32_t groupSize);

    // --- Querying state ---
    bool isLocalLocked(const faabric::Message& msg);

    int getNotifyCount(const faabric::Message& msg);

  private:
    std::shared_mutex sharedMutex;

    std::unordered_map<int32_t, DistributedCoordinationGroup> groups;

    faabric::transport::PointToPointBroker& ptpBroker;

    faabric::util::SystemConfig& conf;

    int32_t timeoutMs = DEFAULT_DISTRIBUTED_TIMEOUT_MS;

    DistributedCoordinationGroup& getCoordinationGroup(int32_t groupId);

    DistributedCoordinationGroup& getOrCreateCoordinationGroup(
      int32_t groupId,
      int32_t groupSize);

    void doLocalNotify(int32_t groupId, int32_t groupSize, bool master);

    void checkGroupIdSet(int32_t groupId);

    void checkGroupSizeSet(int32_t groupSize);
};

DistributedCoordinator& getDistributedCoordinator();

}
