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
  : faabric::transport::MessageEndpointServer(
      STATE_ASYNC_PORT,
      STATE_SYNC_PORT,
      STATE_INPROC_LABEL,
      faabric::util::getSystemConfig().stateServerThreads)
  , state(stateIn)
{}

void StateServer::doAsyncRecv(int header,
                              const uint8_t* buffer,
                              size_t bufferSize)
{
    throw std::runtime_error("State server does not support async recv");
}

std::unique_ptr<google::protobuf::Message>
StateServer::doSyncRecv(int header, const uint8_t* buffer, size_t bufferSize)
{
    switch (header) {
        case faabric::state::StateCalls::Pull: {
            return recvPull(buffer, bufferSize);
        }
        case faabric::state::StateCalls::Push: {
            return recvPush(buffer, bufferSize);
        }
        case faabric::state::StateCalls::Size: {
            return recvSize(buffer, bufferSize);
        }
        case faabric::state::StateCalls::Append: {
            return recvAppend(buffer, bufferSize);
        }
        case faabric::state::StateCalls::ClearAppended: {
            return recvClearAppended(buffer, bufferSize);
        }
        case faabric::state::StateCalls::PullAppended: {
            return recvPullAppended(buffer, bufferSize);
        }
        case faabric::state::StateCalls::Lock: {
            return recvLock(buffer, bufferSize);
        }
        case faabric::state::StateCalls::Unlock: {
            return recvUnlock(buffer, bufferSize);
        }
        case faabric::state::StateCalls::Delete: {
            return recvDelete(buffer, bufferSize);
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized state call header: {}", header));
        }
    }
}

std::unique_ptr<google::protobuf::Message> StateServer::recvSize(
  const uint8_t* buffer,
  size_t bufferSize)
{
    PARSE_MSG(faabric::StateRequest, buffer, bufferSize)

    // Prepare the response
    SPDLOG_TRACE("Received size {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    auto response = std::make_unique<faabric::StateSizeResponse>();
    response->set_user(kv->user);
    response->set_key(kv->key);
    response->set_statesize(kv->size());

    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvPull(
  const uint8_t* buffer,
  size_t bufferSize)
{
    PARSE_MSG(faabric::StateChunkRequest, buffer, bufferSize)

    SPDLOG_TRACE("Received pull {}/{} ({}->{})",
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
  const uint8_t* buffer,
  size_t bufferSize)
{
    PARSE_MSG(faabric::StatePart, buffer, bufferSize)

    // Update the KV store
    SPDLOG_TRACE("Received push {}/{} ({}->{})",
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
  const uint8_t* buffer,
  size_t bufferSize)
{
    PARSE_MSG(faabric::StateRequest, buffer, bufferSize)

    // Update the KV
    KV_FROM_REQUEST(msg)
    auto reqData = BYTES_CONST(msg.data().c_str());
    uint64_t dataLen = msg.data().size();
    kv->append(reqData, dataLen);

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvPullAppended(
  const uint8_t* buffer,
  size_t bufferSize)
{
    PARSE_MSG(faabric::StateAppendedRequest, buffer, bufferSize)

    // Prepare response
    SPDLOG_TRACE("Received pull-appended {}/{}", msg.user(), msg.key());
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
  const uint8_t* buffer,
  size_t bufferSize)
{
    PARSE_MSG(faabric::StateRequest, buffer, bufferSize)

    // Delete value
    SPDLOG_TRACE("Received delete {}/{}", msg.user(), msg.key());
    state.deleteKV(msg.user(), msg.key());

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvClearAppended(
  const uint8_t* buffer,
  size_t bufferSize)
{
    PARSE_MSG(faabric::StateRequest, buffer, bufferSize)

    // Perform operation
    SPDLOG_TRACE("Received clear-appended {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    kv->clearAppended();

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvLock(
  const uint8_t* buffer,
  size_t bufferSize)
{
    PARSE_MSG(faabric::StateRequest, buffer, bufferSize)

    // Perform operation
    SPDLOG_TRACE("Received lock {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    kv->lockGlobal();

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvUnlock(
  const uint8_t* buffer,
  size_t bufferSize)
{
    PARSE_MSG(faabric::StateRequest, buffer, bufferSize)

    // Perform operation
    SPDLOG_TRACE("Received unlock {}/{}", msg.user(), msg.key());
    KV_FROM_REQUEST(msg)
    kv->unlockGlobal();

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}
}
