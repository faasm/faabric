#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointCall.h>
#include <faabric/transport/PointToPointServer.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

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
            PARSE_MSG(faabric::PointToPointMessage,
                      message.udata().data(),
                      message.udata().size())

            // If the sequence number is set, we must also set the ordering
            // flag
            bool mustOrderMsg = sequenceNum != -1;

            // Send the message locally to the downstream socket, add the
            // sequence number for in-order reception
            broker.sendMessage(parsedMsg.groupid(),
                               parsedMsg.sendidx(),
                               parsedMsg.recvidx(),
                               BYTES_CONST(parsedMsg.data().c_str()),
                               parsedMsg.data().size(),
                               mustOrderMsg,
                               sequenceNum);
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
    PARSE_MSG(faabric::PointToPointMessage, buffer.data(), buffer.size())
    SPDLOG_TRACE("Receiving lock on {} for idx {} (recursive {})",
                 parsedMsg.groupid(),
                 parsedMsg.sendidx(),
                 recursive);

    PointToPointGroup::getGroup(parsedMsg.groupid())
      ->lock(parsedMsg.sendidx(), recursive);
}

void PointToPointServer::recvGroupUnlock(std::span<const uint8_t> buffer,
                                         bool recursive)
{
    PARSE_MSG(faabric::PointToPointMessage, buffer.data(), buffer.size())

    SPDLOG_TRACE("Receiving unlock on {} for idx {} (recursive {})",
                 parsedMsg.groupid(),
                 parsedMsg.sendidx(),
                 recursive);

    PointToPointGroup::getGroup(parsedMsg.groupid())
      ->unlock(parsedMsg.sendidx(), recursive);
}

void PointToPointServer::onWorkerStop()
{
    // Clear any thread-local cached sockets
    broker.resetThreadLocalCache();
    broker.clear();
}
}
