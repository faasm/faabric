#include <faabric/state/StateClient.h>
#include <faabric/transport/macros.h>
#include <faabric/util/macros.h>

// TODO - remove
#include <faabric/util/logging.h>

namespace faabric::state {
StateClient::StateClient(const std::string& userIn,
                         const std::string& keyIn,
                         const std::string& hostIn)
  : faabric::transport::MessageEndpointClient(hostIn, STATE_PORT)
  , user(userIn)
  , key(keyIn)
  , host(hostIn)
  , reg(state::getInMemoryStateRegistry())
{
    this->open(faabric::transport::getGlobalMessageContext());
}

void StateClient::sendHeader(faabric::state::StateCalls call)
{
    uint8_t header = static_cast<uint8_t>(call);
    send(&header, sizeof(header), true);
}

faabric::transport::Message StateClient::awaitResponse()
{
    // Call the superclass implementation
    faabric::util::getLogger()->warn("Client awaiting for response");
    return MessageEndpointClient::awaitResponse(STATE_PORT + REPLY_PORT_OFFSET);
}

void StateClient::sendStateRequest(faabric::state::StateCalls header,
                                   bool expectReply)
{
    sendStateRequest(header, nullptr, 0, expectReply);
}

void StateClient::sendStateRequest(faabric::state::StateCalls header,
                                   const uint8_t* data,
                                   int length,
                                   bool expectReply)
{
    faabric::StateRequest request;
    request.set_user(user);
    request.set_key(key);
    if (length > 0) {
        request.set_data(data, length);
    }
    request.set_returnhost(faabric::util::getSystemConfig().endpointHost);
    SEND_MESSAGE(header, request)
}

void StateClient::pushChunks(const std::vector<StateChunk>& chunks)
{
    for (const auto& chunk : chunks) {
        faabric::StatePart stateChunk;
        stateChunk.set_user(user);
        stateChunk.set_key(key);
        stateChunk.set_offset(chunk.offset);
        stateChunk.set_data(chunk.data, chunk.length);
        stateChunk.set_returnhost(
          faabric::util::getSystemConfig().endpointHost);
        SEND_MESSAGE(faabric::state::StateCalls::Push, stateChunk)

        // Await for a response, but discard it as it is empty
        (void)awaitResponse();
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
        request.set_returnhost(faabric::util::getSystemConfig().endpointHost);
        SEND_MESSAGE(faabric::state::StateCalls::Pull, request)

        // Receive message
        faabric::transport::Message recvMsg = awaitResponse();
        PARSE_RESPONSE(faabric::StatePart, recvMsg.data(), recvMsg.size())
        std::copy(response.data().begin(),
                  response.data().end(),
                  bufferStart + response.offset());
    }
}

void StateClient::append(const uint8_t* data, size_t length)
{
    // Send request
    sendStateRequest(faabric::state::StateCalls::Append, data, length);

    // Await for a response, but discard it as it is empty
    (void)awaitResponse();
}

void StateClient::pullAppended(uint8_t* buffer, size_t length, long nValues)
{
    // Prepare request
    faabric::StateAppendedRequest request;
    request.set_user(user);
    request.set_key(key);
    request.set_nvalues(nValues);
    request.set_returnhost(faabric::util::getSystemConfig().endpointHost);
    SEND_MESSAGE(faabric::state::StateCalls::PullAppended, request)

    // Receive response
    faabric::transport::Message recvMsg = awaitResponse();
    PARSE_RESPONSE(
      faabric::StateAppendedResponse, recvMsg.data(), recvMsg.size())

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
    // Send request
    sendStateRequest(faabric::state::StateCalls::ClearAppended);

    // Await for a response, but discard it as it is empty
    (void)awaitResponse();
}

size_t StateClient::stateSize()
{
    // Include the return address in the message body
    sendStateRequest(faabric::state::StateCalls::Size, true);

    // Receive message
    faabric::transport::Message recvMsg = awaitResponse();
    PARSE_RESPONSE(faabric::StateSizeResponse, recvMsg.data(), recvMsg.size())
    return response.statesize();
}

void StateClient::deleteState()
{
    // Send request
    sendStateRequest(faabric::state::StateCalls::Delete, true);

    // Await for a response, but discard it as it is empty
    (void)awaitResponse();
}

void StateClient::lock()
{
    // Send request
    sendStateRequest(faabric::state::StateCalls::Lock);

    // Await for a response, but discard it as it is empty
    (void)awaitResponse();
}

void StateClient::unlock()
{
    sendStateRequest(faabric::state::StateCalls::Unlock);

    // Await for a response, but discard it as it is empty
    (void)awaitResponse();
}
}
