#include <catch.hpp>

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

int doDistributedBarrier(faabric::Message& msg, bool isWorker)
{
    int nChainedFuncs = std::stoi(msg.inputdata());

    // Build up list of state keys used in all cases
    std::vector<std::string> stateKeys;
    for (int i = 0; i < nChainedFuncs; i++) {
        stateKeys.emplace_back("barrier-test-" + std::to_string(i));
    }

    int appIdx = msg.appindex();

    faabric::state::State& state = state::getGlobalState();

    if (!isWorker) {
        // Set up chained messages
        auto chainReq = faabric::util::batchExecFactory(
          msg.user(), "barrier-worker", nChainedFuncs);

        for (int i = 0; i < nChainedFuncs; i++) {
            auto& m = chainReq->mutable_messages()->at(i);

            // Set app index and group data
            m.set_appindex(i);

            m.set_groupid(123);
            m.set_groupindex(i);
            m.set_groupsize(nChainedFuncs);

            m.set_inputdata(msg.inputdata());

            // Set up state for result
            int initialValue = 0;
            state.getKV(m.user(), stateKeys.at(i), sizeof(int32_t))
              ->set(BYTES(&initialValue));
        }

        // Make request and wait for results
        faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
        std::vector<std::string> executedHosts =
          sch.callFunctions(chainReq).hosts;

        bool success = true;
        for (const auto& m : chainReq->messages()) {
            faabric::Message result = sch.getFunctionResult(m.id(), 10000);
            if (result.returnvalue() != 0) {
                SPDLOG_ERROR("Distributed barrier check call failed: {}",
                             m.id());
                success = false;
            }
        }

        return success ? 0 : 1;
    }

    // Sleep for some time
    int waitMs = 500 * appIdx;
    SPDLOG_DEBUG("barrier-worker {} sleeping for {}ms", appIdx, waitMs);
    SLEEP_MS(waitMs);

    // Write result for this thread
    SPDLOG_DEBUG("barrier-worker {} writing result", appIdx);
    std::string stateKey = "barrier-test-" + std::to_string(appIdx);
    std::shared_ptr<faabric::state::StateKeyValue> kv =
      state.getKV(msg.user(), stateKey, sizeof(int32_t));
    kv->set(BYTES(&appIdx));
    kv->pushFull();

    // Wait on a barrier
    SPDLOG_DEBUG("barrier-worker {} waiting on barrier (size {})",
                 appIdx,
                 msg.groupsize());
    faabric::transport::PointToPointGroup::getGroup(msg.groupid())
      ->barrier(msg.appindex());

    // Check that all other values have been set
    for (int i = 0; i < nChainedFuncs; i++) {
        auto idxKv = state.getKV(msg.user(), stateKeys.at(i), sizeof(int32_t));
        uint8_t* idxRawValue = idxKv->get();
        int actualIdxValue = *(int*)idxRawValue;
        if (actualIdxValue != i) {
            SPDLOG_ERROR("barrier-worker check failed on host {}. {} = {}",
                         faabric::util::getSystemConfig().endpointHost,
                         stateKeys.at(i),
                         actualIdxValue);
            return 1;
        }
    }

    return 0;
}

int handleDistributedBarrier(faabric::scheduler::Executor* exec,
                             int threadPoolIdx,
                             int msgIdx,
                             std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);
    return doDistributedBarrier(msg, false);
}

int handleDistributedBarrierWorker(
  faabric::scheduler::Executor* exec,
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);
    return doDistributedBarrier(msg, true);
}
void registerTransportTestFunctions()
{
    registerDistTestExecutorCallback(
      "ptp", "simple", handlePointToPointFunction);

    registerDistTestExecutorCallback(
      "ptp", "barrier", handleDistributedBarrier);

    registerDistTestExecutorCallback(
      "ptp", "barrier-worker", handleDistributedBarrierWorker);
}
}
