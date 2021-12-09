#pragma once

#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace faabric::state {
class InMemoryStateRegistry
{
  public:
    InMemoryStateRegistry() = default;

    std::string getMasterIP(const std::string& user,
                            const std::string& key,
                            const std::string& thisIP,
                            bool claim);

    std::string getMasterIPForOtherMaster(const std::string& userIn,
                                          const std::string& keyIn,
                                          const std::string& thisIP);

    void clear();

  private:
    std::unordered_map<std::string, std::string> masterMap;
    std::shared_mutex masterMapMutex;
};

InMemoryStateRegistry& getInMemoryStateRegistry();
}
