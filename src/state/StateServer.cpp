#include <faabric/state/StateServer.h>

#include <faabric/state/InMemoryStateKeyValue.h>
#include <faabric/state/State.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

#define KV_FROM_REQUEST(request)                                               \
    auto kv = std::static_pointer_cast<InMemoryStateKeyValue>(                 \
      state.getKV(request.user(), request.key()));

namespace faabric::state {
StateServer::StateServer(State& stateIn)
  : faabric::transport::MessageEndpointServer(DEFAULT_STATE_HOST, STATE_PORT)
  , state(stateIn)
{}

void StateServer::sendEmptyResponse(const std::string& returnHost)
{
    faabric::StateResponse response;
    size_t responseSize = response.ByteSizeLong();
    char* serialisedMsg = new char[responseSize];
    if (!response.SerializeToArray(serialisedMsg, responseSize)) {
        throw std::runtime_error("Error serialising message");
    }
    sendResponse(serialisedMsg, responseSize, returnHost, STATE_PORT);
}

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

void StateServer::recvSize(const void* data, int size)
{
    PARSE_MSG(faabric::StateRequest, data, size)

    // Prepare the response
    faabric::util::getLogger()->debug(
      "Size {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
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
    sendResponse(serialisedMsg, responseSize, msg.returnhost(), STATE_PORT);
}

void StateServer::recvPull(const void* data, int size)
{
    PARSE_MSG(faabric::StateChunkRequest, data, size)

    faabric::util::getLogger()->debug("Pull {}/{} ({}->{})",
                                      msg.user(),
                                      msg.key(),
                                      msg.offset(),
                                      msg.offset() + msg.chunksize());

    // Write the response
    faabric::StatePart response;
    KV_FROM_REQUEST(msg)
    uint64_t chunkOffset = msg.offset();
    uint64_t chunkLen = msg.chunksize();
    uint8_t* chunk = kv->getChunk(chunkOffset, chunkLen);
    response.set_user(msg.user());
    response.set_key(msg.key());
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
    sendResponse(serialisedMsg, responseSize, msg.returnhost(), STATE_PORT);
}

void StateServer::recvPush(const void* data, int size)
{
    PARSE_MSG(faabric::StatePart, data, size)

    // Update the KV store
    faabric::util::getLogger()->debug("Push {}/{} ({}->{})",
                                      msg.user(),
                                      msg.key(),
                                      msg.offset(),
                                      msg.offset() +
                                        msg.data().size());
    KV_FROM_REQUEST(msg)
    kv->setChunk(msg.offset(),
                 BYTES_CONST(msg.data().c_str()),
                 msg.data().size());

    sendEmptyResponse(msg.returnhost());
}

void StateServer::recvAppend(const void* data, int size)
{
    PARSE_MSG(faabric::StateRequest, data, size)

    // Update the KV
    KV_FROM_REQUEST(msg)
    auto reqData = BYTES_CONST(msg.data().c_str());
    uint64_t dataLen = msg.data().size();
    kv->append(reqData, dataLen);

    sendEmptyResponse(msg.returnhost());
}

void StateServer::recvPullAppended(const void* data, int size)
{
    PARSE_MSG(faabric::StateAppendedRequest, data, size)

    // Prepare response
    faabric::StateAppendedResponse response;
    faabric::util::getLogger()->debug(
      "Pull appended {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    response.set_user(msg.user());
    response.set_key(msg.key());
    for (uint32_t i = 0; i < msg.nvalues(); i++) {
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
    sendResponse(serialisedMsg, responseSize, msg.returnhost(), STATE_PORT);
}

void StateServer::recvDelete(const void* data, int size)
{
    PARSE_MSG(faabric::StateRequest, data, size)

    // Delete value
    faabric::util::getLogger()->debug(
      "Delete {}/{}", msg.user(), msg.key());
    state.deleteKV(msg.user(), msg.key());

    sendEmptyResponse(msg.returnhost());
}

void StateServer::recvClearAppended(const void* data, int size)
{
    PARSE_MSG(faabric::StateRequest, data, size)

    // Perform operation
    faabric::util::getLogger()->debug(
      "Clear appended {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    kv->clearAppended();

    sendEmptyResponse(msg.returnhost());
}

void StateServer::recvLock(const void* data, int size)
{
    PARSE_MSG(faabric::StateRequest, data, size)

    // Perform operation
    faabric::util::getLogger()->debug(
      "Lock {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    kv->lockWrite();

    sendEmptyResponse(msg.returnhost());
}

void StateServer::recvUnlock(const void* data, int size)
{
    PARSE_MSG(faabric::StateRequest, data, size)

    // Perform operation
    faabric::util::getLogger()->debug(
      "Unlock {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    kv->unlockWrite();

    sendEmptyResponse(msg.returnhost());
}
}
