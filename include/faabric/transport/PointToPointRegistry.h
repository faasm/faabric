#pragma once

#include <faabric/scheduler/Scheduler.h>

#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <set>

namespace faabric::transport {
class PointToPointRegistry
{
  public:
    PointToPointRegistry();

    std::string getHostForReceiver(int appId, int recvIdx);

    void setHostForReceiver(int appId, int recvIdx, const std::string& host);

    void broadcastMappings(int appId);

    void sendMappings(int appId, const std::string &host);

    std::set<int> getIdxsRegisteredForApp(int appId);

    void clear();

  private:
    std::shared_mutex registryMutex;

    std::unordered_map<int, std::set<int>> appIdxs;
    std::unordered_map<std::string, std::string> mappings;

    faabric::scheduler::Scheduler &sch;

    std::string getKey(int appId, int recvIdx);
};

PointToPointRegistry& getPointToPointRegistry();
}
