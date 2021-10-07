#pragma once

#include <faabric/scheduler/Scheduler.h>

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

    void setHostForReceiver(int appId, int recvIdx, const std::string& host);

    void broadcastMappings(int appId);

    void sendMappings(int appId, const std::string& host);

    std::set<int> getIdxsRegisteredForApp(int appId);

    void sendMessage(int appId,
                     int sendIdx,
                     int recvIdx,
                     const uint8_t* buffer,
                     size_t bufferSize);

    std::vector<uint8_t> recvMessage(int appId, int sendIdx, int recvIdx);

    void clear();

  private:
    std::shared_mutex registryMutex;

    std::unordered_map<int, std::set<int>> appIdxs;
    std::unordered_map<std::string, std::string> mappings;

    faabric::scheduler::Scheduler& sch;
};

PointToPointBroker& getPointToPointBroker();
}
