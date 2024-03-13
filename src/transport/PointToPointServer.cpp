#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointCall.h>
#include <faabric/transport/PointToPointMessage.h>
#include <faabric/transport/PointToPointServer.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>

namespace faabric::transport {

PointToPointServer::PointToPointServer()
  : faabric::transport::MessageEndpointServer(
      POINT_TO_POINT_ASYNC_PORT,
      POINT_TO_POINT_SYNC_PORT,
      POINT_TO_POINT_INPROC_LABEL,
      faabric::util::getSystemConfig().pointToPointServerThreads)
  , broker(getPointToPointBroker())
{}

void PointToPointServer::doAsyncRecv(transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    int sequenceNum = message.getSequenceNum();
    switch (header) {
        case (faabric::transport::PointToPointCall::MESSAGE): {
            // Here we are copying the message from the transport layer (NNG)
            // into our PTP message structure
            // NOTE: this mallocs
            PointToPointMessage parsedMsg;
            parsePtpMsg(message.udata(), &parsedMsg);

            // If the sequence number is set, we must also set the ordering
            // flag
            bool mustOrderMsg = sequenceNum != -1;

            // Send the message locally to the downstream socket, add the
            // sequence number for in-order reception
            broker.sendMessage(parsedMsg, mustOrderMsg, sequenceNum);

            // TODO(no-inproc): for the moment, the downstream (inproc)
            // socket makes a copy of this message, so we can free it now
            // after sending. This will not be the case once we move to
            // in-memory queues
            if (parsedMsg.dataPtr != nullptr) {
                faabric::util::free(parsedMsg.dataPtr);
            }
            break;
        }
        case faabric::transport::PointToPointCall::LOCK_GROUP: {
            recvGroupLock(message.udata(), false);
            break;
        }
        case faabric::transport::PointToPointCall::LOCK_GROUP_RECURSIVE: {
            recvGroupLock(message.udata(), true);
            break;
        }
        case faabric::transport::PointToPointCall::UNLOCK_GROUP: {
            recvGroupUnlock(message.udata(), false);
            break;
        }
        case faabric::transport::PointToPointCall::UNLOCK_GROUP_RECURSIVE: {
            recvGroupUnlock(message.udata(), true);
            break;
        }
        default: {
            SPDLOG_ERROR("Invalid aync point-to-point header: {}", header);
            throw std::runtime_error("Invalid async point-to-point message");
        }
    }
}

std::unique_ptr<google::protobuf::Message> PointToPointServer::doSyncRecv(
  transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    switch (header) {
        case (faabric::transport::PointToPointCall::MAPPING): {
            return doRecvMappings(message.udata());
        }
        default: {
            SPDLOG_ERROR("Invalid sync point-to-point header: {}", header);
            throw std::runtime_error("Invalid sync point-to-point message");
        }
    }
}

std::unique_ptr<google::protobuf::Message> PointToPointServer::doRecvMappings(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::PointToPointMappings, buffer.data(), buffer.size())

    faabric::batch_scheduler::SchedulingDecision decision =
      faabric::batch_scheduler::SchedulingDecision::fromPointToPointMappings(
        parsedMsg);

    SPDLOG_DEBUG("Receiving {} point-to-point mappings", decision.nFunctions);

    broker.setUpLocalMappingsFromSchedulingDecision(decision);

    return std::make_unique<faabric::EmptyResponse>();
}

void PointToPointServer::recvGroupLock(std::span<const uint8_t> buffer,
                                       bool recursive)
{
    PointToPointMessage parsedMsg;
    parsePtpMsg(buffer, &parsedMsg);
    assert(parsedMsg.dataPtr == nullptr && parsedMsg.dataSize == 0);

    SPDLOG_TRACE("Receiving lock on {} for idx {} (recursive {})",
                 parsedMsg.groupId,
                 parsedMsg.sendIdx,
                 recursive);

    PointToPointGroup::getGroup(parsedMsg.groupId)
      ->lock(parsedMsg.sendIdx, recursive);
}

void PointToPointServer::recvGroupUnlock(std::span<const uint8_t> buffer,
                                         bool recursive)
{
    PointToPointMessage parsedMsg;
    parsePtpMsg(buffer, &parsedMsg);
    assert(parsedMsg.dataPtr == nullptr && parsedMsg.dataSize == 0);

    SPDLOG_TRACE("Receiving unlock on {} for idx {} (recursive {})",
                 parsedMsg.groupId,
                 parsedMsg.sendIdx,
                 recursive);

    PointToPointGroup::getGroup(parsedMsg.groupId)
      ->unlock(parsedMsg.sendIdx, recursive);
}

void PointToPointServer::onWorkerStop()
{
    // Clear any thread-local cached sockets
    broker.resetThreadLocalCache();
    broker.clear();
}
}
