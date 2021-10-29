#include <catch2/catch.hpp>

#include "DistTestExecutor.h"
#include "faabric_utils.h"
#include "init.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/bytes.h>
#include <faabric/util/string_tools.h>

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

class DistributedCoordinationTestRunner
{
  public:
    DistributedCoordinationTestRunner(faabric::Message& msgIn,
                                      const std::string& statePrefixIn,
                                      int nChainedIn)
      : msg(msgIn)
      , statePrefix(statePrefixIn)
      , nChained(nChainedIn)
      , state(state::getGlobalState())
    {
        for (int i = 0; i < nChained; i++) {
            stateKeys.emplace_back(statePrefix + std::to_string(i));
        }
    }

    std::vector<std::string> getStateKeys() { return stateKeys; }

    std::vector<std::string> setUpStateKeys()
    {
        for (int i = 0; i < nChained; i++) {
            int initialValue = -1;
            state.getKV(msg.user(), stateKeys.at(i), sizeof(int32_t))
              ->set(BYTES(&initialValue));
        }

        return stateKeys;
    }

    void writeResultForIndex()
    {
        int idx = msg.groupidx();

        faabric::state::State& state = state::getGlobalState();
        std::string stateKey = stateKeys.at(idx);
        SPDLOG_DEBUG("{}/{} {} writing result to {}",
                     msg.user(),
                     msg.function(),
                     idx,
                     stateKey);

        std::shared_ptr<faabric::state::StateKeyValue> kv =
          state.getKV(msg.user(), stateKey, sizeof(int32_t));
        kv->set(BYTES(&idx));
        kv->pushFull();
    }

    int callChainedFunc(const std::string& func)
    {
        // Set up chained messages
        auto chainReq =
          faabric::util::batchExecFactory(msg.user(), func, nChained);

        for (int i = 0; i < nChained; i++) {
            auto& m = chainReq->mutable_messages()->at(i);

            // Set app index and group data
            m.set_appid(msg.appid());
            m.set_appidx(i);

            m.set_groupid(groupId);
            m.set_groupidx(i);
            m.set_groupsize(nChained);

            m.set_inputdata(msg.inputdata());
        }

        faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
        sch.callFunctions(chainReq);

        bool success = true;
        for (const auto& m : chainReq->messages()) {
            faabric::Message result = sch.getFunctionResult(m.id(), 10000);
            if (result.returnvalue() != 0) {
                SPDLOG_ERROR("Distributed coordination check call failed: {}",
                             m.id());

                success = false;
            }
        }

        return success ? 0 : 1;
    }

    int checkResults(std::vector<int> expectedResults)
    {
        std::vector<int> actualResults(expectedResults.size(), 0);

        // Load all results
        for (int i = 0; i < expectedResults.size(); i++) {
            auto idxKv =
              state.getKV(msg.user(), stateKeys.at(i), sizeof(int32_t));
            idxKv->pull();

            uint8_t* idxRawValue = idxKv->get();
            int actualIdxValue = *(int*)idxRawValue;
            actualResults.at(i) = actualIdxValue;
        }

        // Check them
        if (actualResults != expectedResults) {
            SPDLOG_ERROR("{}/{} {} check failed on host {} ({} != {})",
                         msg.user(),
                         msg.function(),
                         msg.groupidx(),
                         faabric::util::getSystemConfig().endpointHost,
                         faabric::util::vectorToString<int>(expectedResults),
                         faabric::util::vectorToString<int>(actualResults));
            return 1;
        }

        SPDLOG_DEBUG("{} results for {}/{} ok",
                     expectedResults.size(),
                     msg.user(),
                     msg.function());

        return 0;
    }

  private:
    faabric::Message& msg;
    const std::string statePrefix;
    int nChained = 0;
    faabric::state::State& state;

    std::vector<std::string> stateKeys;

    int groupId = 123;
};

int handleDistributedBarrier(faabric::scheduler::Executor* exec,
                             int threadPoolIdx,
                             int msgIdx,
                             std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);
    int nChainedFuncs = std::stoi(msg.inputdata());

    DistributedCoordinationTestRunner runner(
      msg, "barrier-test-", nChainedFuncs);

    runner.setUpStateKeys();

    // Make request and wait for results
    return runner.callChainedFunc("barrier-worker");
}

int handleDistributedBarrierWorker(
  faabric::scheduler::Executor* exec,
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    DistributedCoordinationTestRunner runner(
      msg, "barrier-test-", msg.groupsize());

    // Sleep for some time
    int groupIdx = msg.groupidx();
    int waitMs = 500 * groupIdx;
    SPDLOG_DEBUG("barrier-worker {} sleeping for {}ms", groupIdx, waitMs);
    SLEEP_MS(waitMs);

    // Write result for this thread
    runner.writeResultForIndex();

    // Wait on a barrier
    SPDLOG_DEBUG("barrier-worker {} waiting on barrier (size {})",
                 groupIdx,
                 msg.groupsize());

    faabric::transport::PointToPointGroup::getGroup(msg.groupid())
      ->barrier(msg.groupidx());

    // At this point all workers should have completed (i.e. everyone has had to
    // wait on the barrier)
    std::vector<int> expectedResults;
    for (int i = 0; i < msg.groupsize(); i++) {
        expectedResults.push_back(i);
    }
    return runner.checkResults(expectedResults);
}

int handleDistributedNotify(faabric::scheduler::Executor* exec,
                            int threadPoolIdx,
                            int msgIdx,
                            std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);
    int nChainedFuncs = std::stoi(msg.inputdata());

    DistributedCoordinationTestRunner runner(
      msg, "notify-test-", nChainedFuncs);

    runner.setUpStateKeys();

    // Make request and wait for results
    return runner.callChainedFunc("notify-worker");
}

int handleDistributedNotifyWorker(
  faabric::scheduler::Executor* exec,
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    DistributedCoordinationTestRunner runner(
      msg, "notify-test-", msg.groupsize());

    // Sleep for some time
    int groupIdx = msg.groupidx();
    int waitMs = 1000 * groupIdx;
    SPDLOG_DEBUG("notify-worker {} sleeping for {}ms", groupIdx, waitMs);
    SLEEP_MS(waitMs);

    int returnValue = 0;
    std::vector<int> expectedResults;
    if (msg.groupidx() == 0) {
        // Master should wait until it's been notified
        faabric::transport::PointToPointGroup::getGroup(msg.groupid())
          ->notify(msg.groupidx());

        // Write result
        runner.writeResultForIndex();

        // Check that all other workers have finished
        for (int i = 0; i < msg.groupsize(); i++) {
            expectedResults.push_back(i);
        }
        returnValue = runner.checkResults(expectedResults);

    } else {
        // Write the result for this worker
        runner.writeResultForIndex();

        // Check results before notifying
        expectedResults = std::vector<int>(msg.groupsize(), -1);
        for (int i = 1; i <= msg.groupidx(); i++) {
            expectedResults.at(i) = i;
        }

        returnValue = runner.checkResults(expectedResults);

        // Notify
        faabric::transport::PointToPointGroup::getGroup(msg.groupid())
          ->notify(msg.groupidx());
    }

    return returnValue;
}

void registerTransportTestFunctions()
{
    registerDistTestExecutorCallback(
      "ptp", "simple", handlePointToPointFunction);

    registerDistTestExecutorCallback(
      "ptp", "barrier", handleDistributedBarrier);

    registerDistTestExecutorCallback(
      "ptp", "barrier-worker", handleDistributedBarrierWorker);

    registerDistTestExecutorCallback("ptp", "notify", handleDistributedNotify);

    registerDistTestExecutorCallback(
      "ptp", "notify-worker", handleDistributedNotifyWorker);
}
}
