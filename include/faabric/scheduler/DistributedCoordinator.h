#pragma once

#include "faabric/util/queue.h"
#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/barrier.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>

#define DEFAULT_DISTRIBUTED_TIMEOUT_MS 30000

namespace faabric::scheduler {

class DistributedLock
{
  public:
    DistributedLock(int32_t groupIdIn, bool recursiveIn);

    void tryLock(int32_t memberIdx);

    void unlock(int32_t memberIdx);

  private:
    int32_t groupId;

    faabric::transport::PointToPointBroker& ptpBroker;

    bool recursive = false;

    std::mutex mx;

    int32_t ownerIdx = -1;

    std::queue<int32_t> lockWaiters;

    void notifyLocked(int32_t memberIdx);
};

class DistributedBarrier
{
  public:
    DistributedBarrier(int32_t groupIdIn, int32_t groupSize);

    void wait(int32_t memberIdx);

  private:
    int32_t groupId;
    int32_t groupSize;

    faabric::transport::PointToPointBroker& ptpBroker;

    std::mutex mx;

    std::set<int32_t> barrierWaiters;
};

class DistributedNotify
{
  public:
    DistributedNotify(int32_t groupIdIn, int32_t groupSize);

    void wait(int32_t memberIdx);

  private:
    int32_t groupId;
    int32_t groupSize;

    std::mutex mx;
    std::atomic<int> count = 0;
    std::condition_variable cv;
};

class DistributedCoordinationGroup
{
  public:
    DistributedCoordinationGroup(int32_t groupIdIn, int32_t groupSizeIn);

    int32_t groupId;
    int32_t groupSize;

    DistributedBarrier barrier;

    DistributedLock lock;
    DistributedLock recursiveLock;

    DistributedNotify notify;
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

    void localLock(int32_t groupId, int32_t groupMember);

    void localLock(int32_t groupId, int32_t groupSize, int32_t groupMember);

    // --- Unlock ---
    void unlock(const faabric::Message& msg);

    void localUnlock(const faabric::Message& msg);

    void localUnlock(int32_t groupId, int32_t groupMember);

    void localUnlock(int32_t groupId, int32_t groupSize, int32_t groupMember);

    // --- Try lock ---
    bool localTryLock(const faabric::Message& msg);

    bool localTryLock(int32_t groupId, int32_t groupSize, int32_t groupMember);

    // --- Lock recursive ---
    void localLockRecursive(const faabric::Message& msg);

    void localLockRecursive(int32_t groupId,
                            int32_t groupSize,
                            int32_t groupMember);

    // --- Unlock recursive
    void localUnlockRecursive(const faabric::Message& msg);

    void localUnlockRecursive(int32_t groupId,
                              int32_t groupSize,
                              int32_t groupMember);

    // --- Notify ---
    void notify(const faabric::Message& msg);

    void localNotify(const faabric::Message& msg);

    void localNotify(int32_t groupId, int32_t groupSize, int32_t groupMember);

    void awaitNotify(const faabric::Message& msg);

    void awaitNotify(int32_t groupId, int32_t groupSize, int32_t groupMember);

    // --- Barrier ---
    void barrier(const faabric::Message& msg);

    void localBarrier(const faabric::Message& msg);

    void localBarrier(int32_t groupId, int32_t groupSize, int32_t groupMember);

    // --- Querying state ---
    bool isLocalLocked(const faabric::Message& msg);

    int getNotifyCount(const faabric::Message& msg);

  private:
    std::shared_mutex sharedMutex;

    std::unordered_map<int32_t, DistributedCoordinationGroup> groups;

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
