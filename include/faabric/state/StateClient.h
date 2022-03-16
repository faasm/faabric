#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/state/InMemoryStateRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::state {
class StateClient : public faabric::transport::MessageEndpointClient
{
  public:
    explicit StateClient(const std::string& userIn,
                         const std::string& keyIn,
                         const std::string& hostIn);

    const std::string user;
    const std::string key;

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
    void sendStateRequest(faabric::state::StateCalls header,
                          const uint8_t* data,
                          int length);

    void logRequest(const std::string &op);
};
}
