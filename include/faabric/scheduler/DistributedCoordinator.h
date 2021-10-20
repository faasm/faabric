#pragma once

#include "faabric/scheduler/FunctionCallClient.h"
#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/barrier.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/queue.h>

#include <stack>

#define DEFAULT_DISTRIBUTED_TIMEOUT_MS 30000

namespace faabric::scheduler {

class DistributedCoordinationGroup
{
  public:
    DistributedCoordinationGroup(const std::string& masterHost,
                                 int32_t groupIdIn,
                                 int32_t groupSizeIn);

    void lock(int32_t groupIdx, bool recursive = false);

    void localLock(bool recursive = false);

    void unlock(int32_t groupIdx, bool recursive = false);

    void localUnlock(bool recursive = false);

    bool localTryLock();

    void barrier(int32_t groupIdx);

    void notify(int32_t groupIdx);

    bool isLocalLocked();

    int32_t getNotifyCount();

  private:
    int32_t timeoutMs = DEFAULT_DISTRIBUTED_TIMEOUT_MS;

    const std::string masterHost;
    int32_t groupId = 0;
    int32_t groupSize = 0;

    bool isMasteredThisHost = true;

    std::mutex mx;

    // Transport
    faabric::scheduler::FunctionCallClient masterClient;
    faabric::transport::PointToPointBroker& ptpBroker;

    // Distributed lock
    std::stack<int32_t> recursiveLockOwners;
    int32_t lockOwnerIdx = -1;
    std::queue<int32_t> lockWaiters;

    void notifyLocked(int32_t memberIdx);

    // Local lock
    std::mutex localMx;
    std::recursive_mutex localRecursiveMx;

    // Distributed barrier
    std::set<int32_t> barrierWaiters;

    void notifyBarrierFinished(int32_t memberIdx);

    // Local barrier
    std::shared_ptr<faabric::util::Barrier> localBarrier = nullptr;

    // Local/ distributed notify
    std::mutex notifyMutex;
    int32_t notifyCount;
    std::condition_variable notifyCv;
};

class DistributedCoordinator
{
  public:
    DistributedCoordinator();

    void clear();

    bool groupExists(int32_t groupId);

    DistributedCoordinationGroup& initGroup(const std::string& masterHost,
                                            int32_t groupId,
                                            int32_t groupSize);

    DistributedCoordinationGroup& initGroup(const faabric::Message& msg);

    DistributedCoordinationGroup& getCoordinationGroup(int32_t groupId);

  private:
    std::shared_mutex sharedMutex;

    std::unordered_map<int32_t, DistributedCoordinationGroup> groups;

    faabric::util::SystemConfig& conf;
};


DistributedCoordinator& getDistributedCoordinator();
}
