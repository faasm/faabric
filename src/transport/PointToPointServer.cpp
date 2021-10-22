#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointCall.h>
#include <faabric/transport/PointToPointServer.h>
#include <faabric/transport/PointToPointBroker.h>
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
            reg.sendMessage(msg.appid(),
                            msg.sendidx(),
                            msg.recvidx(),
                            BYTES_CONST(msg.data().c_str()),
                            msg.data().size());
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

    for (const auto& m : msg.mappings()) {
        // Record the mapping
        reg.setHostForReceiver(m.appid(), m.recvidx(), m.host());
    }

    // Record that this group is now set up on this host
    reg.enableApp(msg.mappings().at(0).appid());

    return std::make_unique<faabric::EmptyResponse>();
}

void PointToPointServer::onWorkerStop()
{
    // Clear any thread-local cached sockets
    reg.resetThreadLocalCache();
}

}
