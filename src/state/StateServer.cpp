#include <faabric/state/StateServer.h>

#include <faabric/rpc/macros.h>
#include <faabric/state/InMemoryStateKeyValue.h>
#include <faabric/state/State.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

#define KV_FROM_REQUEST(request)                                               \
    auto kv = std::static_pointer_cast<InMemoryStateKeyValue>(                 \
      state.getKV(request.user(), request.key()));

namespace faabric::state {
StateServer::StateServer(State& stateIn)
  : faabric::transport::MessageEndpointServer(DEFAULT_RPC_HOST, STATE_PORT)
  , state(stateIn)
{}

void StateServer::doRecv(const void* headerData,
                         int headerSize,
                         const void* bodyData,
                         int bodySize)
{
    int call = static_cast<int>(*static_cast<const char*>(headerData));
    switch (call) {
        case faabric::state::StateCalls::Pull:
            this->recvPull(bodyData, bodySize);
            break;
        case faabric::state::StateCalls::Push:
            this->recvPush(bodyData, bodySize);
            break;
        case faabric::state::StateCalls::Size:
            this->recvSize(bodyData, bodySize);
            break;
        case faabric::state::StateCalls::Append:
            this->recvAppend(bodyData, bodySize);
            break;
        case faabric::state::StateCalls::ClearAppended:
            this->recvClearAppended(bodyData, bodySize);
            break;
        case faabric::state::StateCalls::PullAppended:
            this->recvPullAppended(bodyData, bodySize);
            break;
        case faabric::state::StateCalls::Lock:
            this->recvLock(bodyData, bodySize);
            break;
        case faabric::state::StateCalls::Unlock:
            this->recvUnlock(bodyData, bodySize);
            break;
        case faabric::state::StateCalls::Delete:
            this->recvDelete(bodyData, bodySize);
            break;
        default:
            throw std::runtime_error(
              fmt::format("Unrecognized state call header: {}", call));
    }
}

// Send empty response notifying we are done
void StateServer::sendEmptyResponse(const std::string& returnHost)
{
    faabric::StateResponse response;
    size_t responseSize = response.ByteSizeLong();
    char* serialisedMsg = new char[responseSize];
    if (!response.SerializeToArray(serialisedMsg, responseSize)) {
        throw std::runtime_error("Error serialising message");
    }
    sendResponse(serialisedMsg, responseSize, returnHost);
}

/* Send response to the client
 *
 * Current state implementation _always_ blocks even with methods that return
 * nothing. We create a new endpoint every time. Re-using them would be a
 * possible optimisation if needed.
 */
void StateServer::sendResponse(char* serialisedMsg,
                               int size,
                               const std::string& returnHost)
{
    // Open the endpoint socket, server always binds
    faabric::transport::SimpleMessageEndpoint endpoint(
      returnHost, STATE_PORT + REPLY_PORT_OFFSET);
    endpoint.open(faabric::transport::getGlobalMessageContext(),
                  faabric::transport::SocketType::PUSH,
                  true);
    endpoint.send(serialisedMsg, size);
}

void StateServer::recvSize(const void* data, int size)
{
    // Read input request
    faabric::StateRequest request;

    // Deserialise message string
    if (!request.ParseFromArray(data, size)) {
        throw std::runtime_error("Error deserialising message");
    }

    // Prepare the response
    faabric::util::getLogger()->debug(
      "Size {}/{}", request.user(), request.key());
    KV_FROM_REQUEST(request)
    faabric::StateSizeResponse response;
    response.set_user(kv->user);
    response.set_key(kv->key);
    response.set_statesize(kv->size());

    // Send the response body
    size_t responseSize = response.ByteSizeLong();
    // Deliberately use heap-allocation for zero-copy sending
    char* serialisedMsg = new char[responseSize];
    // Serialise using protobuf
    if (!response.SerializeToArray(serialisedMsg, responseSize)) {
        throw std::runtime_error("Error serialising message");
    }
    sendResponse(serialisedMsg, responseSize, request.returnhost());
}

void StateServer::recvPull(const void* data, int size)
{
    faabric::StateChunkRequest request;

    // Deserialise message string
    if (!request.ParseFromArray(data, size)) {
        throw std::runtime_error("Error deserialising message");
    }

    faabric::util::getLogger()->debug("Pull {}/{} ({}->{})",
                                      request.user(),
                                      request.key(),
                                      request.offset(),
                                      request.offset() + request.chunksize());

    // Write the response
    faabric::StatePart response;
    KV_FROM_REQUEST(request)
    uint64_t chunkOffset = request.offset();
    uint64_t chunkLen = request.chunksize();
    uint8_t* chunk = kv->getChunk(chunkOffset, chunkLen);
    response.set_user(request.user());
    response.set_key(request.key());
    response.set_offset(chunkOffset);
    // TODO: avoid copying here
    response.set_data(chunk, chunkLen);

    // Send the response body
    size_t responseSize = response.ByteSizeLong();
    // Deliberately use heap-allocation for zero-copy sending
    char* serialisedMsg = new char[responseSize];
    // Serialise using protobuf
    if (!response.SerializeToArray(serialisedMsg, responseSize)) {
        throw std::runtime_error("Error serialising message");
    }
    sendResponse(serialisedMsg, responseSize, request.returnhost());
}

void StateServer::recvPush(const void* data, int size)
{
    faabric::StatePart stateChunk;

    // Deserialise message string
    if (!stateChunk.ParseFromArray(data, size)) {
        throw std::runtime_error("Error deserialising message");
    }

    // Update the KV store
    faabric::util::getLogger()->debug("Push {}/{} ({}->{})",
                                      stateChunk.user(),
                                      stateChunk.key(),
                                      stateChunk.offset(),
                                      stateChunk.offset() +
                                        stateChunk.data().size());
    KV_FROM_REQUEST(stateChunk)
    kv->setChunk(stateChunk.offset(),
                 BYTES_CONST(stateChunk.data().c_str()),
                 stateChunk.data().size());

    sendEmptyResponse(stateChunk.returnhost());
}

void StateServer::recvAppend(const void* data, int size)
{
    faabric::StateRequest request;

    // Deserialise message string
    if (!request.ParseFromArray(data, size)) {
        throw std::runtime_error("Error deserialising message");
    }

    // Update the KV
    KV_FROM_REQUEST(request)
    auto reqData = BYTES_CONST(request.data().c_str());
    uint64_t dataLen = request.data().size();
    kv->append(reqData, dataLen);

    sendEmptyResponse(request.returnhost());
}

void StateServer::recvPullAppended(const void* data, int size)
{
    faabric::StateAppendedRequest request;

    // Deserialise message string
    if (!request.ParseFromArray(data, size)) {
        throw std::runtime_error("Error deserialising message");
    }

    // Prepare response
    faabric::StateAppendedResponse response;
    faabric::util::getLogger()->debug(
      "Pull appended {}/{}", request.user(), request.key());
    KV_FROM_REQUEST(request)
    response.set_user(request.user());
    response.set_key(request.key());
    for (uint32_t i = 0; i < request.nvalues(); i++) {
        AppendedInMemoryState& value = kv->getAppendedValue(i);
        auto appendedValue = response.add_values();
        appendedValue->set_data(reinterpret_cast<char*>(value.data.get()),
                                value.length);
    }

    // Send response
    size_t responseSize = response.ByteSizeLong();
    char* serialisedMsg = new char[responseSize];
    if (!response.SerializeToArray(serialisedMsg, responseSize)) {
        throw std::runtime_error("Error serialising message");
    }
    sendResponse(serialisedMsg, responseSize, request.returnhost());
}

void StateServer::recvDelete(const void* data, int size)
{
    faabric::StateRequest request;

    // Deserialise message string
    if (!request.ParseFromArray(data, size)) {
        throw std::runtime_error("Error deserialising message");
    }

    // Delete value
    faabric::util::getLogger()->debug(
      "Delete {}/{}", request.user(), request.key());
    state.deleteKV(request.user(), request.key());

    sendEmptyResponse(request.returnhost());
}

void StateServer::recvClearAppended(const void* data, int size)
{
    faabric::StateRequest request;

    // Deserialise message string
    if (!request.ParseFromArray(data, size)) {
        throw std::runtime_error("Error deserialising message");
    }

    // Perform operation
    faabric::util::getLogger()->debug(
      "Clear appended {}/{}", request.user(), request.key());
    KV_FROM_REQUEST(request)
    kv->clearAppended();

    sendEmptyResponse(request.returnhost());
}

void StateServer::recvLock(const void* data, int size)
{
    faabric::StateRequest request;

    // Deserialise message string
    if (!request.ParseFromArray(data, size)) {
        throw std::runtime_error("Error deserialising message");
    }

    // Perform operation
    faabric::util::getLogger()->debug(
      "Lock {}/{}", request.user(), request.key());
    KV_FROM_REQUEST(request)
    kv->lockWrite();

    sendEmptyResponse(request.returnhost());
}

void StateServer::recvUnlock(const void* data, int size)
{
    faabric::StateRequest request;

    // Deserialise message string
    if (!request.ParseFromArray(data, size)) {
        throw std::runtime_error("Error deserialising message");
    }

    // Perform operation
    faabric::util::getLogger()->debug(
      "Unlock {}/{}", request.user(), request.key());
    KV_FROM_REQUEST(request)
    kv->unlockWrite();

    sendEmptyResponse(request.returnhost());
}
}
