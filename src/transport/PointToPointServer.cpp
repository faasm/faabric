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
  , reg(getPointToPointBroker())
{}

void PointToPointServer::doAsyncRecv(int header,
                                     const uint8_t* buffer,
                                     size_t bufferSize)
{
    switch (header) {
        case (faabric::transport::PointToPointCall::MESSAGE): {
            PARSE_MSG(faabric::PointToPointMessage, buffer, bufferSize)

            // Send the message locally to the downstream socket
            reg.sendMessage(msg.groupid(),
                            msg.sendidx(),
                            msg.recvidx(),
                            BYTES_CONST(msg.data().c_str()),
                            msg.data().size());
            break;
        }
        case faabric::transport::PointToPointCall::LOCK_GROUP: {
            recvCoordinationLock(buffer, bufferSize);
            break;
        }
        case faabric::transport::PointToPointCall::UNLOCK_GROUP: {
            recvCoordinationUnlock(buffer, bufferSize);
            break;
        }
        default: {
            SPDLOG_ERROR("Invalid aync point-to-point header: {}", header);
            throw std::runtime_error("Invalid async point-to-point message");
        }
    }
}

std::unique_ptr<google::protobuf::Message> PointToPointServer::doSyncRecv(
  int header,
  const uint8_t* buffer,
  size_t bufferSize)
{
    switch (header) {
        case (faabric::transport::PointToPointCall::MAPPING): {
            return doRecvMappings(buffer, bufferSize);
        }
        default: {
            SPDLOG_ERROR("Invalid sync point-to-point header: {}", header);
            throw std::runtime_error("Invalid sync point-to-point message");
        }
    }
}

std::unique_ptr<google::protobuf::Message> PointToPointServer::doRecvMappings(
  const uint8_t* buffer,
  size_t bufferSize)
{
    PARSE_MSG(faabric::PointToPointMappings, buffer, bufferSize)

    faabric::util::SchedulingDecision decision =
      faabric::util::SchedulingDecision::fromPointToPointMappings(msg);

    reg.setUpLocalMappingsFromSchedulingDecision(decision);

    return std::make_unique<faabric::EmptyResponse>();
}

void PointToPointServer::recvCoordinationLock(const uint8_t* buffer,
                                              size_t bufferSize)
{
    PARSE_MSG(faabric::PointToPointMessage, buffer, bufferSize)
    SPDLOG_TRACE("Receiving lock on {} for idx {} (recursive {})",
                 msg.groupid(),
                 msg.sendidx());

    reg.getGroup(msg.groupid())->lock(msg.groupidx(), msg.recursive());
}

void PointToPointServer::recvCoordinationUnlock(const uint8_t* buffer,
                                                size_t bufferSize)
{
    PARSE_MSG(faabric::CoordinationRequest, buffer, bufferSize)

    SPDLOG_TRACE("Receiving unlock on {} for idx {} (recursive {})",
                 msg.groupid(),
                 msg.groupidx(),
                 msg.recursive());

    reg.getGroup(msg.groupid())->unlock(msg.groupidx(), msg.recursive());
}

void PointToPointServer::onWorkerStop()
{
    // Clear any thread-local cached sockets
    reg.resetThreadLocalCache();
}

}
