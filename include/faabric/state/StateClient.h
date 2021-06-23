#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/state/InMemoryStateRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/MessageEndpoint.h>

namespace faabric::state {
class StateClient : public faabric::transport::SendMessageEndpoint
{
  public:
    explicit StateClient(const std::string& userIn,
                         const std::string& keyIn,
                         const std::string& hostIn);

    const std::string user;
    const std::string key;
    const std::string host;

    InMemoryStateRegistry& reg;

    /* External state client API */

    void pushChunks(const std::vector<StateChunk>& chunks);

    void pullChunks(const std::vector<StateChunk>& chunks,
                    uint8_t* bufferStart);

    void append(const uint8_t* data, size_t length);

    void pullAppended(uint8_t* buffer, size_t length, long nValues);

    void clearAppended();

    size_t stateSize();

    void deleteState();

    void lock();

    void unlock();

  private:
    void sendHeader(faabric::state::StateCalls call);

    // Block, but ignore return value
    faabric::transport::Message awaitResponse();

    void sendStateRequest(faabric::state::StateCalls header, bool expectReply);

    void sendStateRequest(faabric::state::StateCalls header,
                          const uint8_t* data = nullptr,
                          int length = 0,
                          bool expectReply = false);
};
}
