#include <faabric/state/StateClient.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

namespace faabric::state {
StateClient::StateClient(const std::string& userIn,
                         const std::string& keyIn,
                         const std::string& hostIn)
  : faabric::transport::MessageEndpointClient(hostIn,
                                              STATE_ASYNC_PORT,
                                              STATE_SYNC_PORT)
  , user(userIn)
  , key(keyIn)
{}

void StateClient::sendStateRequest(faabric::state::StateCalls header,
                                   const uint8_t* data,
                                   int length)
{
    faabric::StateRequest request;
    request.set_user(user);
    request.set_key(key);

    if (length > 0) {
        request.set_data(data, length);
    }

    faabric::EmptyResponse resp;
    syncSend(header, data, length, &resp);
}

void StateClient::pushChunks(const std::vector<StateChunk>& chunks)
{
    for (const auto& chunk : chunks) {
        faabric::StatePart stateChunk;
        stateChunk.set_user(user);
        stateChunk.set_key(key);
        stateChunk.set_offset(chunk.offset);
        stateChunk.set_data(chunk.data, chunk.length);

        faabric::EmptyResponse resp;
        syncSend(faabric::state::StateCalls::Push, &stateChunk, &resp);
    }
}

void StateClient::pullChunks(const std::vector<StateChunk>& chunks,
                             uint8_t* bufferStart)
{
    for (const auto& chunk : chunks) {
        // Prepare request
        faabric::StateChunkRequest request;
        request.set_user(user);
        request.set_key(key);
        request.set_offset(chunk.offset);
        request.set_chunksize(chunk.length);

        // Send and copy response into place
        faabric::StatePart response;
        syncSend(faabric::state::StateCalls::Pull, &request, &response);
        std::copy(response.data().begin(),
                  response.data().end(),
                  bufferStart + response.offset());
    }
}

void StateClient::append(const uint8_t* data, size_t length)
{
    sendStateRequest(faabric::state::StateCalls::Append, data, length);
}

void StateClient::pullAppended(uint8_t* buffer, size_t length, long nValues)
{
    // Prepare request
    faabric::StateAppendedRequest request;
    request.set_user(user);
    request.set_key(key);
    request.set_nvalues(nValues);

    faabric::StateAppendedResponse response;
    syncSend(faabric::state::StateCalls::PullAppended, &request, &response);

    // Process response
    size_t offset = 0;
    for (auto& value : response.values()) {
        if (offset > length) {
            throw std::runtime_error(fmt::format(
              "Buffer not large enough for appended data (offset={}, length{})",
              offset,
              length));
        }

        auto valueData = BYTES_CONST(value.data().c_str());
        std::copy(valueData, valueData + value.data().size(), buffer + offset);
        offset += value.data().size();
    }
}

void StateClient::clearAppended()
{
    sendStateRequest(faabric::state::StateCalls::ClearAppended, nullptr, 0);
}

size_t StateClient::stateSize()
{
    faabric::StateRequest request;
    request.set_user(user);
    request.set_key(key);

    faabric::StateSizeResponse response;
    syncSend(faabric::state::StateCalls::Size, nullptr, 0, &response);

    return response.statesize();
}

void StateClient::deleteState()
{
    sendStateRequest(faabric::state::StateCalls::Delete, nullptr, 0);
}

void StateClient::lock()
{
    sendStateRequest(faabric::state::StateCalls::Lock, nullptr, 0);
}

void StateClient::unlock()
{
    sendStateRequest(faabric::state::StateCalls::Unlock, nullptr, 0);
}
}
