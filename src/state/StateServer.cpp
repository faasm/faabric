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

void StateServer::doAsyncRecv(transport::Message& message)
{
    throw std::runtime_error("State server does not support async recv");
}

std::unique_ptr<google::protobuf::Message> StateServer::doSyncRecv(
  transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    switch (header) {
        case faabric::state::StateCalls::Pull: {
            return recvPull(message.udata());
        }
        case faabric::state::StateCalls::Push: {
            return recvPush(message.udata());
        }
        case faabric::state::StateCalls::Size: {
            return recvSize(message.udata());
        }
        case faabric::state::StateCalls::Append: {
            return recvAppend(message.udata());
        }
        case faabric::state::StateCalls::ClearAppended: {
            return recvClearAppended(message.udata());
        }
        case faabric::state::StateCalls::PullAppended: {
            return recvPullAppended(message.udata());
        }
        case faabric::state::StateCalls::Delete: {
            return recvDelete(message.udata());
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized state call header: {}", header));
        }
    }
}

std::unique_ptr<google::protobuf::Message> StateServer::recvSize(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::StateRequest, buffer.data(), buffer.size())

    // Prepare the response
    SPDLOG_TRACE("Received size {}/{}", parsedMsg.user(), parsedMsg.key());
    KV_FROM_REQUEST(parsedMsg)
    auto response = std::make_unique<faabric::StateSizeResponse>();
    response->set_user(kv->user);
    response->set_key(kv->key);
    response->set_statesize(kv->size());

    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvPull(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::StateChunkRequest, buffer.data(), buffer.size())

    SPDLOG_TRACE("Received pull {}/{} ({}->{})",
                 parsedMsg.user(),
                 parsedMsg.key(),
                 parsedMsg.offset(),
                 parsedMsg.offset() + parsedMsg.chunksize());

    // Write the response
    KV_FROM_REQUEST(parsedMsg)
    uint64_t chunkOffset = parsedMsg.offset();
    uint64_t chunkLen = parsedMsg.chunksize();
    uint8_t* chunk = kv->getChunk(chunkOffset, chunkLen);

    auto response = std::make_unique<faabric::StatePart>();
    response->set_user(parsedMsg.user());
    response->set_key(parsedMsg.key());
    response->set_offset(chunkOffset);
    // TODO: avoid copying here
    response->set_data(chunk, chunkLen);

    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvPush(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::StatePart, buffer.data(), buffer.size())

    // Update the KV store
    SPDLOG_TRACE("Received push {}/{} ({}->{})",
                 parsedMsg.user(),
                 parsedMsg.key(),
                 parsedMsg.offset(),
                 parsedMsg.offset() + parsedMsg.data().size());

    KV_FROM_REQUEST(parsedMsg)
    kv->setChunk(parsedMsg.offset(),
                 BYTES_CONST(parsedMsg.data().c_str()),
                 parsedMsg.data().size());

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvAppend(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::StateRequest, buffer.data(), buffer.size())

    // Update the KV
    KV_FROM_REQUEST(parsedMsg)
    auto reqData = BYTES_CONST(parsedMsg.data().c_str());
    uint64_t dataLen = parsedMsg.data().size();
    kv->append(reqData, dataLen);

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvPullAppended(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::StateAppendedRequest, buffer.data(), buffer.size())

    // Prepare response
    SPDLOG_TRACE(
      "Received pull-appended {}/{}", parsedMsg.user(), parsedMsg.key());
    KV_FROM_REQUEST(parsedMsg)

    auto response = std::make_unique<faabric::StateAppendedResponse>();
    response->set_user(parsedMsg.user());
    response->set_key(parsedMsg.key());
    for (uint32_t i = 0; i < parsedMsg.nvalues(); i++) {
        AppendedInMemoryState& value = kv->getAppendedValue(i);
        auto appendedValue = response->add_values();
        appendedValue->set_data(reinterpret_cast<char*>(value.data.get()),
                                value.length);
    }

    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvDelete(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::StateRequest, buffer.data(), buffer.size())

    // Delete value
    SPDLOG_TRACE("Received delete {}/{}", parsedMsg.user(), parsedMsg.key());
    state.deleteKV(parsedMsg.user(), parsedMsg.key());

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}

std::unique_ptr<google::protobuf::Message> StateServer::recvClearAppended(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::StateRequest, buffer.data(), buffer.size())

    // Perform operation
    SPDLOG_TRACE(
      "Received clear-appended {}/{}", parsedMsg.user(), parsedMsg.key());
    KV_FROM_REQUEST(parsedMsg)
    kv->clearAppended();

    auto response = std::make_unique<faabric::StateResponse>();
    return response;
}
}
