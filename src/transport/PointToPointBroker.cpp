#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointRegistry.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

namespace faabric::transport {

std::string getPointToPointInprocLabel(int appId, int sendIdx, int recvIdx)
{
    return fmt::format("{}-{}-{}", appId, sendIdx, recvIdx);
}

PointToPointBroker::PointToPointBroker()
  : faabric::transport::MessageEndpointServer(
      POINT_TO_POINT_ASYNC_PORT,
      POINT_TO_POINT_SYNC_PORT,
      POINT_TO_POINT_INPROC_LABEL,
      faabric::util::getSystemConfig().pointToPointBrokerThreads)
{}

void PointToPointBroker::doAsyncRecv(int header,
                                     const uint8_t* buffer,
                                     size_t bufferSize)
{
    PARSE_MSG(faabric::PointToPointMessage, buffer, bufferSize)

    std::unique_ptr<AsyncInternalSendMessageEndpoint> endpoint =
      getSendEndpoint(msg.appid(), msg.sendidx(), msg.recvidx());

    SPDLOG_TRACE("Forwarding point-to-point message {}:{}:{} to {}",
                 msg.appid(),
                 msg.sendidx(),
                 msg.recvidx(),
                 endpoint->getAddress());

    // TODO - is this copying the data? Would be nice to avoid if poss
    endpoint->send(BYTES_CONST(msg.data().c_str()), msg.data().size());
}

std::unique_ptr<google::protobuf::Message> PointToPointBroker::doSyncRecv(
  int header,
  const uint8_t* buffer,
  size_t bufferSize)
{
    PARSE_MSG(faabric::PointToPointMappings, buffer, bufferSize)

    PointToPointRegistry& reg = getPointToPointRegistry();

    for (const auto& m : msg.mappings()) {
        reg.setHostForReceiver(m.appid(), m.recvidx(), m.host());
    }

    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<AsyncInternalSendMessageEndpoint>
PointToPointBroker::getSendEndpoint(int appId, int sendIdx, int recvIdx)
{
    std::string label = getPointToPointInprocLabel(appId, sendIdx, recvIdx);
    return std::make_unique<AsyncInternalSendMessageEndpoint>(label);
}

}
