#pragma once

#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>

#include <atomic>
#include <barrier>
#include <condition_variable>
#include <queue>
#include <set>
#include <shared_mutex>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#define DEFAULT_DISTRIBUTED_TIMEOUT_MS 30000

#define POINT_TO_POINT_MAIN_IDX 0

namespace faabric::transport {

class PointToPointBroker;

class PointToPointGroup
{
  public:
    static std::shared_ptr<PointToPointGroup> getGroup(int groupId);

    static std::shared_ptr<PointToPointGroup> getOrAwaitGroup(int groupId);

    static bool groupExists(int groupId);

    static void addGroup(int appId,
                         int groupId,
                         int groupSize,
                         bool isSingleHost);

    static void addGroupIfNotExists(int appId, int groupId, int groupSize);

    static void clearGroup(int groupId);

    static void clear();

    PointToPointGroup(int appId,
                      int groupIdIn,
                      int groupSizeIn,
                      bool isSingleHostIn);

    void lock(int groupIdx, bool recursive);

    void unlock(int groupIdx, bool recursive);

    int getLockOwner(bool recursive);

    void localLock();

    void localUnlock();

    bool localTryLock();

    void barrier(int groupIdx);

    void notify(int groupIdx);

    int getNotifyCount();

  private:
    faabric::util::SystemConfig& conf;

    std::string mainHost;
    int appId = 0;
    int groupId = 0;
    int groupSize = 0;
    // Hint to speed-up local operations
    bool isSingleHost;

    std::shared_mutex mx;

    // Transport
    faabric::transport::PointToPointBroker& ptpBroker;

    // Local lock
    std::timed_mutex localMx;
    std::recursive_timed_mutex localRecursiveMx;

    // Local barrier
    std::barrier<void (*)()> localBarrier;

    // Distributed lock
    std::stack<int> recursiveLockOwners;
    std::atomic<int> lockOwnerIdx = -1;
    std::queue<int> lockWaiters;

    void notifyLocked(int groupIdx);
};

class PointToPointBroker
{
  public:
    PointToPointBroker();

    std::string getHostForReceiver(int groupId, int recvIdx);

    int getMpiPortForReceiver(int groupId, int recvIdx);

    std::set<std::string> setUpLocalMappingsFromSchedulingDecision(
      const faabric::batch_scheduler::SchedulingDecision& decision);

    void setAndSendMappingsFromSchedulingDecision(
      const faabric::batch_scheduler::SchedulingDecision& decision);

    void sendMappingsFromSchedulingDecision(
      const faabric::batch_scheduler::SchedulingDecision& decision,
      const std::set<std::string>& hostList);

    void waitForMappingsOnThisHost(int groupId);

    std::set<int> getIdxsRegisteredForGroup(int groupId);

    void updateHostForIdx(int groupId, int groupIdx, std::string newHost);

    void sendMessage(int groupId,
                     int sendIdx,
                     int recvIdx,
                     const uint8_t* buffer,
                     size_t bufferSize,
                     std::string hostHint,
                     bool mustOrderMsg = false);

    void sendMessage(int groupId,
                     int sendIdx,
                     int recvIdx,
                     const uint8_t* buffer,
                     size_t bufferSize,
                     bool mustOrderMsg = false,
                     int sequenceNum = NO_SEQUENCE_NUM,
                     std::string hostHint = "");

    std::vector<uint8_t> recvMessage(int groupId,
                                     int sendIdx,
                                     int recvIdx,
                                     bool mustOrderMsg = false);

    void clearGroup(int groupId);

    void clear();

    void resetThreadLocalCache();

    void postMigrationHook(int groupId, int groupIdx);

  private:
    faabric::util::SystemConfig& conf;

    std::shared_mutex brokerMutex;

    std::unordered_map<int, std::set<int>> groupIdIdxsMap;
    std::unordered_map<std::string, std::string> mappings;
    std::unordered_map<std::string, int> mpiPortMappings;

    std::unordered_map<int, std::shared_ptr<faabric::util::FlagWaiter>>
      groupFlags;

    std::shared_ptr<faabric::util::FlagWaiter> getGroupFlag(int groupId);

    Message doRecvMessage(int groupId, int sendIdx, int recvIdx);

    void initSequenceCounters(int groupId);

    int getAndIncrementSentMsgCount(int groupId, int recvIdx);

    void incrementRecvMsgCount(int groupId, int sendIdx);

    int getExpectedSeqNum(int groupId, int sendIdx);
};

PointToPointBroker& getPointToPointBroker();
}
