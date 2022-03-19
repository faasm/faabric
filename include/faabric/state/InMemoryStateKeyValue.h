#pragma once

#include <faabric/state/InMemoryStateRegistry.h>
#include <faabric/state/StateClient.h>
#include <faabric/state/StateKeyValue.h>

#include <faabric/proto/faabric.pb.h>

#include <faabric/util/clock.h>

namespace faabric::state {
enum InMemoryStateKeyStatus
{
    NOT_MASTER,
    MASTER,
};

class AppendedInMemoryState
{
  public:
    AppendedInMemoryState(size_t lengthIn, std::unique_ptr<uint8_t[]>&& dataIn)
      : length(lengthIn)
      , data(std::move(dataIn))
    {}

    size_t length;
    std::unique_ptr<uint8_t[]> data;
};

class InMemoryStateKeyValue final : public StateKeyValue
{
  public:
    InMemoryStateKeyValue(const std::string& userIn,
                          const std::string& keyIn,
                          size_t sizeIn,
                          const std::string& thisIPIn);

    InMemoryStateKeyValue(const std::string& userIn,
                          const std::string& keyIn,
                          const std::string& thisIPIn);

    static size_t getStateSizeFromRemote(const std::string& userIn,
                                         const std::string& keyIn,
                                         const std::string& thisIPIn);

    static void deleteFromRemote(const std::string& userIn,
                                 const std::string& keyIn,
                                 const std::string& thisIPIn);

    static void clearAll(bool global);

    bool isMaster();

    AppendedInMemoryState& getAppendedValue(uint idx);

  private:
    const std::string thisIP;
    const std::string masterIP;
    InMemoryStateKeyStatus status;

    InMemoryStateRegistry& stateRegistry;

    std::vector<AppendedInMemoryState> appendedData;

    void pullFromRemote() override;

    void pullChunkFromRemote(long offset, size_t length) override;

    void pushToRemote() override;

    void pushPartialToRemote(
      const std::vector<StateChunk>& dirtyChunks) override;

    void appendToRemote(const uint8_t* data, size_t length) override;

    void pullAppendedFromRemote(uint8_t* data,
                                size_t length,
                                long nValues) override;

    void clearAppendedFromRemote() override;
};
}
