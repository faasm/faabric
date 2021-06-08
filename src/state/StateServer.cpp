#include <faabric/state/StateServer.h>

#include <faabric/state/InMemoryStateKeyValue.h>
#include <faabric/state/State.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/macros.h>

#define KV_FROM_REQUEST(request)                                               \
    auto kv = std::static_pointer_cast<InMemoryStateKeyValue>(                 \
      state.getKV(request.user(), request.key()));

namespace faabric::state {
StateServer::StateServer(State& stateIn)
  : faabric::transport::MessageEndpointServer(STATE_PORT)
  , state(stateIn)
{}

void StateServer::doRecv(faabric::transport::Message& header,
                         faabric::transport::Message& body)
{
    assert(header.size() == sizeof(uint8_t));
    uint8_t call = static_cast<uint8_t>(*header.data());
    switch (call) {
        case faabric::state::StateCalls::Pull:
            this->recvPull(body);
            break;
        case faabric::state::StateCalls::Push:
            this->recvPush(body);
            break;
        case faabric::state::StateCalls::Size:
            this->recvSize(body);
            break;
        case faabric::state::StateCalls::Append:
            this->recvAppend(body);
            break;
        case faabric::state::StateCalls::ClearAppended:
            this->recvClearAppended(body);
            break;
        case faabric::state::StateCalls::PullAppended:
            this->recvPullAppended(body);
            break;
        case faabric::state::StateCalls::Lock:
            this->recvLock(body);
            break;
        case faabric::state::StateCalls::Unlock:
            this->recvUnlock(body);
            break;
        case faabric::state::StateCalls::Delete:
            this->recvDelete(body);
            break;
        default:
            throw std::runtime_error(
              fmt::format("Unrecognized state call header: {}", call));
    }
}

void StateServer::recvSize(faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateRequest, body.data(), body.size())

    // Prepare the response
    SPDLOG_TRACE("Size {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    faabric::StateSizeResponse response;
    response.set_user(kv->user);
    response.set_key(kv->key);
    response.set_statesize(kv->size());
    SEND_SERVER_RESPONSE(response, msg.returnhost(), STATE_PORT)
}

void StateServer::recvPull(faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateChunkRequest, body.data(), body.size())

    SPDLOG_TRACE("Pull {}/{} ({}->{})",
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
    SEND_SERVER_RESPONSE(response, msg.returnhost(), STATE_PORT)
}

void StateServer::recvPush(faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StatePart, body.data(), body.size())

    // Update the KV store
    SPDLOG_TRACE("Push {}/{} ({}->{})",
                msg.user(),
                msg.key(),
                msg.offset(),
                msg.offset() + msg.data().size());
    KV_FROM_REQUEST(msg)
    kv->setChunk(
      msg.offset(), BYTES_CONST(msg.data().c_str()), msg.data().size());

    faabric::StateResponse emptyResponse;
    SEND_SERVER_RESPONSE(emptyResponse, msg.returnhost(), STATE_PORT)
}

void StateServer::recvAppend(faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateRequest, body.data(), body.size())

    // Update the KV
    KV_FROM_REQUEST(msg)
    auto reqData = BYTES_CONST(msg.data().c_str());
    uint64_t dataLen = msg.data().size();
    kv->append(reqData, dataLen);

    faabric::StateResponse emptyResponse;
    SEND_SERVER_RESPONSE(emptyResponse, msg.returnhost(), STATE_PORT)
}

void StateServer::recvPullAppended(faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateAppendedRequest, body.data(), body.size())

    // Prepare response
    faabric::StateAppendedResponse response;
    SPDLOG_TRACE("Pull appended {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    response.set_user(msg.user());
    response.set_key(msg.key());
    for (uint32_t i = 0; i < msg.nvalues(); i++) {
        AppendedInMemoryState& value = kv->getAppendedValue(i);
        auto appendedValue = response.add_values();
        appendedValue->set_data(reinterpret_cast<char*>(value.data.get()),
                                value.length);
    }
    SEND_SERVER_RESPONSE(response, msg.returnhost(), STATE_PORT)
}

void StateServer::recvDelete(faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateRequest, body.data(), body.size())

    // Delete value
    SPDLOG_TRACE("Delete {}/{}", msg.user(), msg.key());
    state.deleteKV(msg.user(), msg.key());

    faabric::StateResponse emptyResponse;
    SEND_SERVER_RESPONSE(emptyResponse, msg.returnhost(), STATE_PORT)
}

void StateServer::recvClearAppended(faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateRequest, body.data(), body.size())

    // Perform operation
    SPDLOG_TRACE("Clear appended {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    kv->clearAppended();

    faabric::StateResponse emptyResponse;
    SEND_SERVER_RESPONSE(emptyResponse, msg.returnhost(), STATE_PORT)
}

void StateServer::recvLock(faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateRequest, body.data(), body.size())

    // Perform operation
    SPDLOG_TRACE("Lock {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    kv->lockWrite();

    faabric::StateResponse emptyResponse;
    SEND_SERVER_RESPONSE(emptyResponse, msg.returnhost(), STATE_PORT)
}

void StateServer::recvUnlock(faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateRequest, body.data(), body.size())

    // Perform operation
    SPDLOG_TRACE("Unlock {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    kv->unlockWrite();

    faabric::StateResponse emptyResponse;
    SEND_SERVER_RESPONSE(emptyResponse, msg.returnhost(), STATE_PORT)
}
}
