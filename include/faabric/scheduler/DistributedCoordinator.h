#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
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
    DistributedCoordinationGroup(const std::string& masterHostIn,
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

    void overrideMasterHost(const std::string& host);

  private:
    int32_t timeoutMs = DEFAULT_DISTRIBUTED_TIMEOUT_MS;

    std::string masterHost;
    int32_t groupId = 0;
    int32_t groupSize = 0;

    bool isMasteredThisHost = true;

    std::mutex mx;

    // Transport
    faabric::scheduler::FunctionCallClient masterClient;
    faabric::transport::PointToPointBroker& ptpBroker;

    // Local lock
    std::timed_mutex localMx;
    std::recursive_timed_mutex localRecursiveMx;

    // Distributed lock
    std::stack<int32_t> recursiveLockOwners;
    int32_t lockOwnerIdx = -1;
    std::queue<int32_t> lockWaiters;

    void notifyLocked(int32_t groupIdx);
};

class DistributedCoordinator
{
  public:
    DistributedCoordinator();

    void clear();

    bool groupExists(int32_t groupId);

    std::shared_ptr<DistributedCoordinationGroup> initGroup(
      const std::string& masterHost,
      int32_t groupId,
      int32_t groupSize);

    std::shared_ptr<DistributedCoordinationGroup> initGroup(
      const faabric::Message& msg);

    std::shared_ptr<DistributedCoordinationGroup> getCoordinationGroup(
      int32_t groupId);

  private:
    std::shared_mutex sharedMutex;

    std::unordered_map<int32_t, std::shared_ptr<DistributedCoordinationGroup>>
      groups;

    faabric::util::SystemConfig& conf;
};

DistributedCoordinator& getDistributedCoordinator();
}
