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

    int groupId = msg.groupid();
    uint8_t groupIdx = (uint8_t)msg.groupidx();

    faabric::transport::PointToPointBroker& broker =
      faabric::transport::getPointToPointBroker();

    // Send to next index in ring and recv from previous in ring.
    uint8_t minIdx = 0;
    uint8_t maxIdx = 3;
    uint8_t sendToIdx = groupIdx < maxIdx ? groupIdx + 1 : minIdx;
    uint8_t recvFromIdx = groupIdx > minIdx ? groupIdx - 1 : maxIdx;

    // Send a series of our own index, expect to receive the same from other
    // senders
    std::vector<uint8_t> sendData(10, groupIdx);
    std::vector<uint8_t> expectedRecvData(10, recvFromIdx);

    // Do the sending
    broker.sendMessage(
      groupId, groupIdx, sendToIdx, sendData.data(), sendData.size());

    // Do the receiving
    std::vector<uint8_t> actualRecvData =
      broker.recvMessage(groupId, recvFromIdx, groupIdx);

    // Check data is as expected
    if (actualRecvData != expectedRecvData) {
        SPDLOG_ERROR("Point-to-point recv data not as expected {} != {}",
                     formatByteArrayToIntString(actualRecvData),
                     formatByteArrayToIntString(expectedRecvData));
        return 1;
    }

    return 0;
}

std::vector<std::string> getStateKeys(int nChainedFuncs)
{
    std::vector<std::string> stateKeys;
    for (int i = 0; i < nChainedFuncs; i++) {
        stateKeys.emplace_back("barrier-test-" + std::to_string(i));
    }

    return stateKeys;
}

int handleDistributedBarrier(faabric::scheduler::Executor* exec,
                             int threadPoolIdx,
                             int msgIdx,
                             std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);
    int nChainedFuncs = std::stoi(msg.inputdata());

    std::vector<std::string> stateKeys = getStateKeys(nChainedFuncs);
    faabric::state::State& state = state::getGlobalState();

    // Set up chained messages
    auto chainReq = faabric::util::batchExecFactory(
      msg.user(), "barrier-worker", nChainedFuncs);

    for (int i = 0; i < nChainedFuncs; i++) {
        auto& m = chainReq->mutable_messages()->at(i);

        // Set app index and group data
        m.set_appid(msg.appid());
        m.set_appidx(i);

        m.set_groupid(123);
        m.set_groupidx(i);
        m.set_groupsize(nChainedFuncs);

        m.set_inputdata(msg.inputdata());

        // Set up state for result
        int initialValue = 0;
        state.getKV(m.user(), stateKeys.at(i), sizeof(int32_t))
          ->set(BYTES(&initialValue));
    }

    // Make request and wait for results
    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.callFunctions(chainReq);

    bool success = true;
    for (const auto& m : chainReq->messages()) {
        faabric::Message result = sch.getFunctionResult(m.id(), 10000);
        if (result.returnvalue() != 0) {
            SPDLOG_ERROR("Distributed barrier check call failed: {}", m.id());
            success = false;
        }
    }

    return success ? 0 : 1;
}

int handleDistributedBarrierWorker(
  faabric::scheduler::Executor* exec,
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);
    int groupIdx = msg.groupidx();

    std::vector<std::string> stateKeys = getStateKeys(msg.groupsize());
    faabric::state::State& state = state::getGlobalState();

    std::string stateKey = stateKeys.at(groupIdx);

    // Sleep for some time
    int waitMs = 500 * groupIdx;
    SPDLOG_DEBUG("barrier-worker {} sleeping for {}ms", groupIdx, waitMs);
    SLEEP_MS(waitMs);

    // Write result for this thread
    SPDLOG_DEBUG("barrier-worker {} writing result to {}", groupIdx, stateKey);
    std::shared_ptr<faabric::state::StateKeyValue> kv =
      state.getKV(msg.user(), stateKey, sizeof(int32_t));
    kv->set(BYTES(&groupIdx));
    kv->pushFull();

    // Wait on a barrier
    SPDLOG_DEBUG("barrier-worker {} waiting on barrier (size {})",
                 groupIdx,
                 msg.groupsize());

    faabric::transport::PointToPointGroup::getGroup(msg.groupid())
      ->barrier(msg.groupidx());

    // Check that all other values have been set
    for (int i = 0; i < msg.groupsize(); i++) {
        auto idxKv = state.getKV(msg.user(), stateKeys.at(i), sizeof(int32_t));
        idxKv->pull();

        uint8_t* idxRawValue = idxKv->get();
        int actualIdxValue = *(int*)idxRawValue;
        if (actualIdxValue != i) {
            SPDLOG_ERROR("barrier-worker {} check failed on host {}. {} = {}",
                         groupIdx,
                         faabric::util::getSystemConfig().endpointHost,
                         stateKeys.at(i),
                         actualIdxValue);
            return 1;
        }
    }

    return 0;
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
