#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointServer.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

namespace faabric::transport {
PointToPointServer::PointToPointServer()
  : faabric::transport::MessageEndpointServer(POINT_TO_POINT_ASYNC_PORT,
                                              POINT_TO_POINT_SYNC_PORT)
{}

void PointToPointServer::doAsyncRecv(int header,
                                     const uint8_t* buffer,
                                     size_t bufferSize)
{
    PARSE_MSG(faabric::PointToPointMessage, buffer, bufferSize)

    std::shared_ptr<AsyncSendMessageEndpoint> endpoint =
      getSendEndpoint(msg.appid(), msg.sendidx(), msg.recvidx());

    // TODO - is this copying the data? Would be nice to avoid if poss
    endpoint->send(BYTES_CONST(msg.data().c_str()), msg.data().size());
}

std::unique_ptr<google::protobuf::Message> PointToPointServer::doSyncRecv(
  int header,
  const uint8_t* buffer,
  size_t bufferSize)
{
    std::string errMsg = "Point-to-point sync messaging not supported";
    SPDLOG_ERROR(errMsg);
    throw std::runtime_error(errMsg);
}

std::vector<uint8_t> PointToPointServer::recvMessage(int appId,
                                                     int sendIdx,
                                                     int recvIdx)
{
    std::shared_ptr<AsyncRecvMessageEndpoint> endpoint =
      getRecvEndpoint(appId, sendIdx, recvIdx);

    std::optional<Message> messageDataMaybe = endpoint->recv().value();
    Message& messageData = messageDataMaybe.value();

    // TODO - possible to avoid this copy too?
    return messageData.dataCopy();
}

std::string PointToPointServer::getInprocLabel(int appId,
                                               int sendIdx,
                                               int recvIdx)
{
    return fmt::format("{}-{}-{}", appId, sendIdx, recvIdx);
}
}
