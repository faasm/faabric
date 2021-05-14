#include <faabric/state/StateClient.h>
#include <faabric/transport/macros.h>
#include <faabric/util/macros.h>

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
    this->open(faabric::transport::getGlobalMessageContext(),
               faabric::transport::SocketType::PUSH,
               false);
}

StateClient::~StateClient()
{
    close();
}

void StateClient::sendHeader(faabric::state::StateCalls call)
{
    // Deliberately using heap allocation, so that ZeroMQ can use zero-copy
    int functionNum = static_cast<int>(call);
    size_t headerSize = sizeof(faabric::state::StateCalls);
    char* header = new char[headerSize];
    memcpy(header, &functionNum, headerSize);
    // Mark that we are sending more messages
    send(header, headerSize, true);
}

// Block until call finishes, but ignore response
void StateClient::awaitResponse()
{
    char* data;
    int size;
    awaitResponse(data, size);
    delete data;
}

void StateClient::awaitResponse(char*& data, int& size)
{
    // Call the superclass implementation
    MessageEndpointClient::awaitResponse(
      faabric::util::getSystemConfig().endpointHost,
      STATE_PORT + REPLY_PORT_OFFSET,
      data,
      size);
}

void StateClient::sendStateRequest(faabric::state::StateCalls header, bool expectReply)
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

        // Await for a response
        awaitResponse();
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
        char* msgData;
        int size;
        awaitResponse(msgData, size);
        faabric::StatePart response;
        if (!response.ParseFromArray(msgData, size)) {
            throw std::runtime_error("Error deserialising message");
        }
        delete msgData;
        std::copy(response.data().begin(),
                  response.data().end(),
                  bufferStart + response.offset());
    }
}

void StateClient::append(const uint8_t* data, size_t length)
{
    // Send request
    sendStateRequest(faabric::state::StateCalls::Append, data, length);

    // Await for response to finish
    awaitResponse();
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
    faabric::StateAppendedResponse response;
    char* msgData;
    int size;
    awaitResponse(msgData, size);
    if (!response.ParseFromArray(msgData, size)) {
        throw std::runtime_error("Error deserialising message");
    }
    delete msgData;

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

    // Await for response to finish
    awaitResponse();
}

size_t StateClient::stateSize()
{
    // Include the return address in the message body
    sendStateRequest(faabric::state::StateCalls::Size, true);

    faabric::StateSizeResponse response;
    // Receive message
    char* msgData;
    int size;
    awaitResponse(msgData, size);
    if (!response.ParseFromArray(msgData, size)) {
        throw std::runtime_error("Error deserialising message");
    }
    delete msgData;
    return response.statesize();
}

void StateClient::deleteState()
{
    // Send request
    sendStateRequest(faabric::state::StateCalls::Delete, true);

    // Wait for the response
    awaitResponse();
}

void StateClient::lock()
{
    // Send request
    sendStateRequest(faabric::state::StateCalls::Lock);

    // Await for response to finish
    awaitResponse();
}

void StateClient::unlock()
{
    sendStateRequest(faabric::state::StateCalls::Unlock);

    // Await for response to finish
    awaitResponse();
}
}
