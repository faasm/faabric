#include <catch2/catch.hpp>

#include "DistTestExecutor.h"
#include "faabric_utils.h"
#include "init.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/bytes.h>

using namespace faabric::util;

namespace tests {

int handlePointToPointFunction(
  faabric::scheduler::Executor* exec,
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    uint8_t appIdx = (uint8_t)msg.appindex();

    faabric::transport::PointToPointBroker& broker =
      faabric::transport::getPointToPointBroker();

    // Send to next index in ring and recv from previous in ring.
    uint8_t minIdx = 1;
    uint8_t maxIdx = 3;
    uint8_t sendToIdx = appIdx < maxIdx ? appIdx + 1 : minIdx;
    uint8_t recvFromIdx = appIdx > minIdx ? appIdx - 1 : maxIdx;

    // Send a series of our own index, expect to receive the same from other
    // senders
    std::vector<uint8_t> sendData(10, appIdx);
    std::vector<uint8_t> expectedRecvData(10, recvFromIdx);

    // Do the sending
    broker.sendMessage(
      msg.appid(), appIdx, sendToIdx, sendData.data(), sendData.size());

    // Do the receiving
    std::vector<uint8_t> actualRecvData =
      broker.recvMessage(msg.appid(), recvFromIdx, appIdx);

    // Check data is as expected
    if (actualRecvData != expectedRecvData) {
        SPDLOG_ERROR("Point-to-point recv data not as expected {} != {}",
                     formatByteArrayToIntString(actualRecvData),
                     formatByteArrayToIntString(expectedRecvData));
        return 1;
    }

    return 0;
}

void registerTransportTestFunctions()
{
    registerDistTestExecutorCallback(
      "ptp", "simple", handlePointToPointFunction);
}
}
