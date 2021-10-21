#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/transport/PointToPointBroker.h>

#include <queue>
#include <stack>

#define DEFAULT_DISTRIBUTED_TIMEOUT_MS 30000

namespace faabric::scheduler {

class DistributedCoordinationGroup
{
  public:
    DistributedCoordinationGroup(const std::string& masterHostIn,
                                 int32_t groupIdIn,
                                 int32_t groupSizeIn);

    void lock(int32_t groupIdx, bool recursive);

    void unlock(int32_t groupIdx, bool recursive);

    int32_t getLockOwner(bool recursive);

    void localLock();

    void localUnlock();

    bool localTryLock();

    void barrier(int32_t groupIdx);

    void notify(int32_t groupIdx);

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
}
