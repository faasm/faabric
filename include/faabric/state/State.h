#pragma once

#include <faabric/state/StateKeyValue.h>

#include <shared_mutex>
#include <string>

namespace faabric::state {

// State client-server API
enum StateCalls
{
    NoStateCall = 0,
    Pull = 1,
    Push = 2,
    Size = 3,
    Append = 4,
    ClearAppended = 5,
    PullAppended = 6,
    Lock = 7,
    Unlock = 8,
    Delete = 9,
};

class State
{
  public:
    explicit State(std::string thisIPIn);

    size_t getStateSize(const std::string& user, const std::string& keyIn);

    std::shared_ptr<StateKeyValue> getKV(const std::string& user,
                                         const std::string& key,
                                         size_t size);

    std::shared_ptr<StateKeyValue> getKV(const std::string& user,
                                         const std::string& key);

    void forceClearAll(bool global);

    void deleteKV(const std::string& userIn, const std::string& keyIn);

    void deleteKVLocally(const std::string& userIn, const std::string& keyIn);

    size_t getKVCount();

    std::string getThisIP();

  private:
    const std::string thisIP;

    std::unordered_map<std::string, std::shared_ptr<StateKeyValue>> kvMap;
    std::shared_mutex mapMutex;

    std::shared_ptr<StateKeyValue> doGetKV(const std::string& user,
                                           const std::string& key,
                                           bool sizeless,
                                           size_t size);
};

State& getGlobalState();
}
