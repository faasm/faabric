#include <faabric/state/InMemoryStateKeyValue.h>
#include <faabric/state/State.h>
#include <faabric/state/StateServer.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

#define KV_FROM_REQUEST(request)                                               \
    auto kv = std::static_pointer_cast<InMemoryStateKeyValue>(                 \
      state.getKV(request.user(), request.key()));

namespace faabric::state {
StateServer::StateServer(State& stateIn)
  : faabric::transport::MessageEndpointServer(STATE_ASYNC_PORT, STATE_SYNC_PORT)
  , state(stateIn)
{}

void StateServer::doAsyncRecv(faabric::transport::Message& header,
                              faabric::transport::Message& body)
{
    throw std::runtime_error("State server does not support async recv");
}

std::unique_ptr<google::protobuf::Message> StateServer::doSyncRecv(
  faabric::transport::Message& header,
  faabric::transport::Message& body)
{
    assert(header.size() == sizeof(uint8_t));

    uint8_t call = static_cast<uint8_t>(*header.data());
    switch (call) {
        case faabric::state::StateCalls::Pull: {
            return recvPull(body);
        }
        case faabric::state::StateCalls::Push: {
            return recvPush(body);
        }
        case faabric::state::StateCalls::Size: {
            return recvSize(body);
        }
        case faabric::state::StateCalls::Append: {
            return recvAppend(body);
        }
        case faabric::state::StateCalls::ClearAppended: {
            return recvClearAppended(body);
        }
        case faabric::state::StateCalls::PullAppended: {
            return recvPullAppended(body);
        }
        case faabric::state::StateCalls::Lock: {
            return recvLock(body);
        }
        case faabric::state::StateCalls::Unlock: {
            return recvUnlock(body);
        }
        case faabric::state::StateCalls::Delete: {
            return recvDelete(body);
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized state call header: {}", call));
        }
    }
}

std::unique_ptr<google::protobuf::Message> StateServer::recvSize(
  faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateRequest, body.data(), body.size())

    // Prepare the response
    SPDLOG_TRACE("Size {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    auto response = std::make_unique<faabric::StateSizeResponse>();
    response->set_user(kv->user);
    response->set_key(kv->key);
    response->set_statesize(kv->size());

    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvPull(
  faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateChunkRequest, body.data(), body.size())

    SPDLOG_TRACE("Pull {}/{} ({}->{})",
                 msg.user(),
                 msg.key(),
                 msg.offset(),
                 msg.offset() + msg.chunksize());

    // Write the response
    KV_FROM_REQUEST(msg)
    uint64_t chunkOffset = msg.offset();
    uint64_t chunkLen = msg.chunksize();
    uint8_t* chunk = kv->getChunk(chunkOffset, chunkLen);

    auto response = std::make_unique<faabric::StatePart>();
    response->set_user(msg.user());
    response->set_key(msg.key());
    response->set_offset(chunkOffset);
    // TODO: avoid copying here
    response->set_data(chunk, chunkLen);

    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvPush(
  faabric::transport::Message& body)
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

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvAppend(
  faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateRequest, body.data(), body.size())

    // Update the KV
    KV_FROM_REQUEST(msg)
    auto reqData = BYTES_CONST(msg.data().c_str());
    uint64_t dataLen = msg.data().size();
    kv->append(reqData, dataLen);

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvPullAppended(
  faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateAppendedRequest, body.data(), body.size())

    // Prepare response
    SPDLOG_TRACE("Pull appended {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)

    auto response = std::make_unique<faabric::StateAppendedResponse>();
    response->set_user(msg.user());
    response->set_key(msg.key());
    for (uint32_t i = 0; i < msg.nvalues(); i++) {
        AppendedInMemoryState& value = kv->getAppendedValue(i);
        auto appendedValue = response->add_values();
        appendedValue->set_data(reinterpret_cast<char*>(value.data.get()),
                                value.length);
    }

    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvDelete(
  faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateRequest, body.data(), body.size())

    // Delete value
    SPDLOG_TRACE("Delete {}/{}", msg.user(), msg.key());
    state.deleteKV(msg.user(), msg.key());

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvClearAppended(
  faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateRequest, body.data(), body.size())

    // Perform operation
    SPDLOG_TRACE("Clear appended {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    kv->clearAppended();

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvLock(
  faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateRequest, body.data(), body.size())

    // Perform operation
    SPDLOG_TRACE("Lock {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    kv->lockWrite();

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvUnlock(
  faabric::transport::Message& body)
{
    PARSE_MSG(faabric::StateRequest, body.data(), body.size())

    // Perform operation
    SPDLOG_TRACE("Unlock {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    kv->unlockWrite();

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}
}
