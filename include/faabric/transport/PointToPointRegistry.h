#pragma once

#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace faabric::transport {
class PointToPointRegistry
{
  public:
    PointToPointRegistry();

    std::string getHostForReceiver(int appId, int recvIdx);

    void setHostForReceiver(int appId, int recvIdx, const std::string& host);

    void broadcastMappings(int appId, std::vector<int> indexes);

  private:
    std::shared_mutex registryMutex;

    std::unordered_map<std::string, std::string> mappings;

    std::string getKey(int appId, int recvIdx);
};

PointToPointRegistry& getPointToPointRegistry();
}
