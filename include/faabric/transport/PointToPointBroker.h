#pragma once

#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/util/scheduling.h>

#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace faabric::transport {
class PointToPointBroker
{
  public:
    PointToPointBroker();

    std::string getHostForReceiver(int appId, int recvIdx);

    std::set<std::string> setUpLocalMappingsFromSchedulingDecision(
      const faabric::util::SchedulingDecision& decision);

    void setAndSendMappingsFromSchedulingDecision(
      const faabric::util::SchedulingDecision& decision);

    void waitForAppToBeEnabled(int appId, int recvIdx);

    std::set<int> getIdxsRegisteredForApp(int appId);

    void sendMessage(int appId,
                     int sendIdx,
                     int recvIdx,
                     const uint8_t* buffer,
                     size_t bufferSize);

    std::vector<uint8_t> recvMessage(int appId, int sendIdx, int recvIdx);

    void clear();

    void resetThreadLocalCache();

  private:
    std::shared_mutex brokerMutex;

    std::unordered_map<int, std::set<int>> appIdxs;
    std::unordered_map<std::string, std::string> mappings;

    std::shared_ptr<PointToPointClient> getClient(const std::string& host);

    faabric::scheduler::Scheduler& sch;

    void enableApp(int appId);
};

PointToPointBroker& getPointToPointBroker();
}
