#pragma once

#include <faabric/transport/PointToPointClient.h>
#include <faabric/util/config.h>
#include <faabric/util/scheduling.h>

#include <queue>
#include <set>
#include <shared_mutex>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#define DEFAULT_DISTRIBUTED_TIMEOUT_MS 30000

namespace faabric::transport {

class PointToPointBroker;

class PointToPointGroup
{
  public:
    PointToPointGroup(const std::string& masterHostIn,
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
    faabric::transport::PointToPointClient masterClient;
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

    std::shared_ptr<PointToPointGroup> getGroup(int groupId);

    std::vector<uint8_t> recvMessage(int groupId, int sendIdx, int recvIdx);

    void clear();

    void resetThreadLocalCache();

  private:
    std::shared_mutex brokerMutex;

    std::unordered_map<int, std::set<int>> groupIdIdxsMap;
    std::unordered_map<std::string, std::string> mappings;

    std::unordered_map<int, bool> groupMappingsFlags;
    std::unordered_map<int, std::mutex> groupMappingMutexes;
    std::unordered_map<int, std::condition_variable> groupMappingCvs;

    std::shared_ptr<PointToPointClient> getClient(const std::string& host);

    std::unordered_map<int32_t, std::shared_ptr<PointToPointGroup>> groups;

    faabric::util::SystemConfig& conf;
};

PointToPointBroker& getPointToPointBroker();
}
