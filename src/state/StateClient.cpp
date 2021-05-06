#include <faabric/state/StateClient.h>
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

void StateClient::sendStateRequest(bool expectReply)
{
    sendStateRequest(nullptr, 0, expectReply);
}

void StateClient::sendStateRequest(const uint8_t* data,
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
    size_t requestSize = request.ByteSizeLong();
    char* serialisedMsg = new char[requestSize];
    // Serialise using protobuf
    if (!request.SerializeToArray(serialisedMsg, requestSize)) {
        throw std::runtime_error("Error serialising message");
    }
    send(serialisedMsg, requestSize);
}

/* Note - this was a streaming RPC that we now turn into a series of consecutive
 * pull requests to the server.
 */
void StateClient::pushChunks(const std::vector<StateChunk>& chunks)
{
    for (const auto& chunk : chunks) {
        // Send the header first
        sendHeader(faabric::state::StateCalls::Push);

        faabric::StatePart stateChunk;
        stateChunk.set_user(user);
        stateChunk.set_key(key);
        stateChunk.set_offset(chunk.offset);
        stateChunk.set_data(chunk.data, chunk.length);
        stateChunk.set_returnhost(
          faabric::util::getSystemConfig().endpointHost);
        size_t chunkSize = stateChunk.ByteSizeLong();
        char* serialisedMsg = new char[chunkSize];
        // Serialise using protobuf
        if (!stateChunk.SerializeToArray(serialisedMsg, chunkSize)) {
            throw std::runtime_error("Error serialising message");
        }
        send(serialisedMsg, chunkSize);

        // Await for a response
        awaitResponse();
    }
}

/* Note - this was a streaming RPC that we now turn into a series of consecutive
 * pull requests to the server.
 */
void StateClient::pullChunks(const std::vector<StateChunk>& chunks,
                             uint8_t* bufferStart)
{
    for (const auto& chunk : chunks) {
        // Send the header first
        sendHeader(faabric::state::StateCalls::Pull);

        // Prepare request
        faabric::StateChunkRequest request;
        request.set_user(user);
        request.set_key(key);
        request.set_offset(chunk.offset);
        request.set_chunksize(chunk.length);
        request.set_returnhost(faabric::util::getSystemConfig().endpointHost);
        size_t requestSize = request.ByteSizeLong();
        char* serialisedMsg = new char[requestSize];
        // Serialise using protobuf
        if (!request.SerializeToArray(serialisedMsg, requestSize)) {
            throw std::runtime_error("Error serialising message");
        }
        send(serialisedMsg, requestSize);

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
    // Send the header first
    sendHeader(faabric::state::StateCalls::Append);

    // Send request
    sendStateRequest(data, length);

    // Await for response to finish
    awaitResponse();
}

void StateClient::pullAppended(uint8_t* buffer, size_t length, long nValues)
{
    // Send the header first
    sendHeader(faabric::state::StateCalls::PullAppended);

    // Prepare request
    faabric::StateAppendedRequest request;
    request.set_user(user);
    request.set_key(key);
    request.set_nvalues(nValues);
    request.set_returnhost(faabric::util::getSystemConfig().endpointHost);
    size_t requestSize = request.ByteSizeLong();
    char* serialisedMsg = new char[requestSize];
    // Serialise using protobuf
    if (!request.SerializeToArray(serialisedMsg, requestSize)) {
        throw std::runtime_error("Error serialising message");
    }
    send(serialisedMsg, requestSize);

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
    // Send the header first
    sendHeader(faabric::state::StateCalls::ClearAppended);

    // Send request
    sendStateRequest();

    // Await for response to finish
    awaitResponse();
}

size_t StateClient::stateSize()
{
    // Send the header first
    sendHeader(faabric::state::StateCalls::Size);

    // Include the return address in the message body
    sendStateRequest(true);

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
    // Send the header first
    sendHeader(faabric::state::StateCalls::Delete);

    // Send request
    sendStateRequest(true);

    // Wait for the response
    awaitResponse();
}

void StateClient::lock()
{
    // Send the header first
    sendHeader(faabric::state::StateCalls::Lock);

    // Send request
    sendStateRequest();

    // Await for response to finish
    awaitResponse();
}

void StateClient::unlock()
{
    // Send the header first
    sendHeader(faabric::state::StateCalls::Unlock);

    sendStateRequest();

    // Await for response to finish
    awaitResponse();
}

/*
void StateClient::doRecv(void* msgData, int size)
{
    throw std::runtime_error("Calling recv from a producer client.");
}
*/
}
