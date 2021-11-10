#pragma once

#include <faabric/transport/PointToPointClient.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/scheduling.h>

#include <condition_variable>
#include <queue>
#include <set>
#include <shared_mutex>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#define DEFAULT_DISTRIBUTED_TIMEOUT_MS 30000

#define POINT_TO_POINT_MASTER_IDX 0

namespace faabric::transport {

class PointToPointBroker;

class PointToPointGroup
{
  public:
    static std::shared_ptr<PointToPointGroup> getGroup(int groupId);

    static std::shared_ptr<PointToPointGroup> getOrAwaitGroup(int groupId);

    static bool groupExists(int groupId);

    static void addGroup(int appId, int groupId, int groupSize);

    static void addGroupIfNotExists(int appId, int groupId, int groupSize);

    static void clearGroup(int groupId);

    static void clear();

    PointToPointGroup(int appId, int groupIdIn, int groupSizeIn);

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

    int timeoutMs = DEFAULT_DISTRIBUTED_TIMEOUT_MS;

    std::string masterHost;
    int appId = 0;
    int groupId = 0;
    int groupSize = 0;

    std::mutex mx;

    // Transport
    faabric::transport::PointToPointBroker& ptpBroker;

    // Local lock
    std::timed_mutex localMx;
    std::recursive_timed_mutex localRecursiveMx;

    // Distributed lock
    std::stack<int> recursiveLockOwners;
    int lockOwnerIdx = -1;
    std::queue<int> lockWaiters;

    void notifyLocked(int groupIdx);
};

class PointToPointBroker
{
  public:
    PointToPointBroker();

    std::string getHostForReceiver(int groupId, int recvIdx);

    std::set<std::string> setUpLocalMappingsFromSchedulingDecision(
      const faabric::util::SchedulingDecision& decision);

    void setAndSendMappingsFromSchedulingDecision(
      const faabric::util::SchedulingDecision& decision);

    void waitForMappingsOnThisHost(int groupId);

    std::set<int> getIdxsRegisteredForGroup(int groupId);

    void sendMessage(int groupId,
                     int sendIdx,
                     int recvIdx,
                     const uint8_t* buffer,
                     size_t bufferSize);

    std::vector<uint8_t> recvMessage(int groupId, int sendIdx, int recvIdx);

    void clearGroup(int groupId);

    void clear();

    void resetThreadLocalCache();

  private:
    faabric::util::SystemConfig& conf;

    std::shared_mutex brokerMutex;

    std::unordered_map<int, std::set<int>> groupIdIdxsMap;
    std::unordered_map<std::string, std::string> mappings;

    std::unordered_map<int, faabric::util::FlagWaiter> groupFlags;

    faabric::util::FlagWaiter& getGroupFlag(int groupId);
};

PointToPointBroker& getPointToPointBroker();
}
