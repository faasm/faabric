#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointRegistry.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

namespace faabric::transport {

PointToPointBroker::PointToPointBroker()
  : faabric::transport::MessageEndpointServer(
      POINT_TO_POINT_ASYNC_PORT,
      POINT_TO_POINT_SYNC_PORT,
      POINT_TO_POINT_INPROC_LABEL,
      faabric::util::getSystemConfig().pointToPointBrokerThreads)
  , reg(getPointToPointRegistry())
{}

void PointToPointBroker::doAsyncRecv(int header,
                                     const uint8_t* buffer,
                                     size_t bufferSize)
{
    PARSE_MSG(faabric::PointToPointMessage, buffer, bufferSize)

    reg.sendMessage(msg.appid(),
                    msg.sendidx(),
                    msg.recvidx(),
                    BYTES_CONST(msg.data().c_str()),
                    msg.data().size());
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

}
