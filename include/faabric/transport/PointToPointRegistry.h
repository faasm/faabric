#pragma once

#include <mutex>
#include <string>

namespace faabric::transport {
class PointToPointRegistry
{
  public:
    PointToPointRegistry();

    std::string getHostForReceiver(int appId, int recvIdx);

    void setHostForReceiver(int appId, int recvIdx, const std::string& host);

  private:
    std::mutex registryMutex;
};
}
