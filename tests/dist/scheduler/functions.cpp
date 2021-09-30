#include "faabric_utils.h"
#include <catch.hpp>

#include "DistTestExecutor.h"
#include "init.h"

#include <sys/mman.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/DistributedCoordinator.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/bytes.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>

namespace tests {

int handleSimpleThread(faabric::scheduler::Executor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    // Return a distinctive value
    int returnValue = msg.id() / 2;

    const faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    SPDLOG_DEBUG("Thread {} executed on host {}. Returning {}",
                 msg.id(),
                 conf.endpointHost,
                 returnValue);

    return returnValue;
}

int handleSimpleFunction(faabric::scheduler::Executor* exec,
                         int threadPoolIdx,
                         int msgIdx,
                         std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);
    const faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    std::string output = fmt::format(
      "Function {} executed on host {}", msg.id(), conf.endpointHost);

    SPDLOG_DEBUG(output);

    msg.set_outputdata(output);

    return 0;
}

int handleFakeDiffsFunction(faabric::scheduler::Executor* exec,
                            int threadPoolIdx,
                            int msgIdx,
                            std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    faabric::util::SnapshotData snap = exec->snapshot();

    std::string msgInput = msg.inputdata();
    std::string snapshotKey = msg.snapshotkey();

    // Modify the executor's memory
    std::vector<uint8_t> inputBytes = faabric::util::stringToBytes(msgInput);
    std::vector<uint8_t> keyBytes = faabric::util::stringToBytes(snapshotKey);

    uint32_t offsetA = 10;
    uint32_t offsetB = 100;

    std::memcpy(snap.data + offsetA, keyBytes.data(), keyBytes.size());
    std::memcpy(snap.data + offsetB, inputBytes.data(), inputBytes.size());

    return 123;
}

int handleFakeDiffsThreadedFunction(
  faabric::scheduler::Executor* exec,
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    bool isThread = req->type() == faabric::BatchExecuteRequest::THREADS;
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);
    std::string snapshotKey = "fake-diffs-threaded-snap";
    std::string msgInput = msg.inputdata();

    // This function creates a snapshot, then spawns some child threads that
    // will modify the shared memory. It then awaits the results and checks that
    // the modifications are synced back to the original host.
    if (!isThread) {
        int nThreads = std::stoi(msgInput);

        // Set up the snapshot
        size_t snapSize = (nThreads * 4) * faabric::util::HOST_PAGE_SIZE;
        uint8_t* snapMemory = (uint8_t*)mmap(
          nullptr, snapSize, PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

        faabric::util::SnapshotData snap;
        snap.data = snapMemory;
        snap.size = snapSize;

        faabric::snapshot::SnapshotRegistry& reg =
          faabric::snapshot::getSnapshotRegistry();
        reg.takeSnapshot(snapshotKey, snap);

        auto req =
          faabric::util::batchExecFactory(msg.user(), msg.function(), nThreads);
        req->set_type(faabric::BatchExecuteRequest::THREADS);

        for (int i = 0; i < nThreads; i++) {
            auto& m = req->mutable_messages()->at(i);
            m.set_appindex(i);
            m.set_inputdata(std::string("thread_" + std::to_string(i)));
            m.set_snapshotkey(snapshotKey);

            // Make a small modification to a page that will also be edited by
            // the child thread to make sure it's not overwritten
            std::vector<uint8_t> localChange(3, i);
            uint32_t offset = 2 * i * faabric::util::HOST_PAGE_SIZE;
            std::memcpy(
              snapMemory + offset, localChange.data(), localChange.size());
        }

        // Dispatch the message, expecting them all to execute on other hosts
        std::string thisHost = faabric::util::getSystemConfig().endpointHost;
        faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
        std::vector<std::string> executedHosts = sch.callFunctions(req);

        bool rightHosts = true;
        for (auto& h : executedHosts) {
            if (h == thisHost) {
                SPDLOG_ERROR("Expected child threads to be executed on other "
                             "hosts (this host {}, actual host {})",
                             thisHost,
                             h);
                rightHosts = false;
            }
        }

        if (!rightHosts) {
            return 111;
        }

        // Wait for the threads
        for (auto& m : req->messages()) {
            sch.awaitThreadResult(m.id());
        }

        // Check that the changes have been made to the snapshot memory
        bool success = true;
        for (int i = 0; i < nThreads; i++) {
            // Check local modifications
            std::vector<uint8_t> expectedLocal(3, i);
            uint32_t localOffset = 2 * i * faabric::util::HOST_PAGE_SIZE;
            std::vector<uint8_t> actualLocal(snapMemory + localOffset,
                                             snapMemory + localOffset +
                                               expectedLocal.size());

            if (actualLocal != expectedLocal) {
                SPDLOG_ERROR("Local modifications not present for {}", i);
                success = false;
            }

            // Check remote modifications
            uint32_t offset = 2 * i * faabric::util::HOST_PAGE_SIZE + 10;
            std::string expectedData("thread_" + std::to_string(i));
            auto* charPtr = reinterpret_cast<char*>(snapMemory + offset);
            std::string actual(charPtr);

            if (actual != expectedData) {
                SPDLOG_ERROR(
                  "Diff not as expected. {} != {}", actual, expectedData);
                success = false;
            }
        }

        if (!success) {
            return 222;
        }

    } else {
        int idx = msg.appindex();
        uint32_t offset = 2 * idx * faabric::util::HOST_PAGE_SIZE + 10;

        // Modify the executor's memory
        std::vector<uint8_t> inputBytes =
          faabric::util::stringToBytes(msgInput);

        faabric::util::SnapshotData snap = exec->snapshot();
        std::memcpy(snap.data + offset, inputBytes.data(), inputBytes.size());

        return 0;
    }

    return 333;
}

int handleDistributedBarrier(faabric::scheduler::Executor* exec,
                             int threadPoolIdx,
                             int msgIdx,
                             std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    bool isThread = req->type() == faabric::BatchExecuteRequest::THREADS;
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    int nThreads = 4;

    // Build up list of state keys used in all cases
    std::vector<std::string> stateKeys;
    for (int i = 0; i < nThreads; i++) {
        stateKeys.emplace_back("barrier-test-" + std::to_string(i));
    }

    faabric::state::State& state = state::getGlobalState();

    if (!isThread) {
        // Set up chained messages
        auto chainReq =
          faabric::util::batchExecFactory(msg.user(), msg.function(), nThreads);
        for (int i = 0; i < chainReq->messages_size(); i++) {
            auto& msg = chainReq->mutable_messages()->at(i);

            // Set app index and group data
            msg.set_appindex(i);
            msg.set_groupid(123);
            msg.set_groupsize(nThreads);

            // Set up state for result
            int initialValue = 0;
            state.getKV(msg.user(), stateKeys.at(i), sizeof(int32_t))
              ->set(BYTES(&initialValue));
        }

        // Make request and wait for results
        faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
        std::vector<std::string> executedHosts = sch.callFunctions(req);
        bool success = true;
        for (const auto& m : req->messages()) {
            int32_t thisRes = sch.awaitThreadResult(m.id());
            if (thisRes != 0) {
                SPDLOG_ERROR("Distributed barrier check call failed: {}",
                             m.id());
                success = false;
            }
        }

        return success ? 0 : 1;
    } else {
        int appIdx = msg.appindex();

        // Sleep for some time
        int waitMs = 500 * appIdx;
        SLEEP_MS(waitMs);

        // Write result for this thread
        std::string stateKey = "barrier-test-" + std::to_string(appIdx);
        state.getKV(msg.user(), stateKey, sizeof(int32_t))->set(BYTES(&appIdx));

        // Wait on a barrier
        faabric::scheduler::DistributedCoordinator& sync =
          faabric::scheduler::getDistributedCoordinator();
        sync.barrier(msg);

        // Check that all other values have been set
        for (int i = 0; i < nThreads; i++) {
            auto idxKv =
              state.getKV(msg.user(), stateKeys.at(i), sizeof(int32_t));
            uint8_t* idxRawValue = idxKv->get();
            int actualIdxValue = *(int*)idxRawValue;
            if (actualIdxValue != i) {
                SPDLOG_ERROR(
                  "Distributed barrier check failed on host {}. {} = {}",
                  faabric::util::getSystemConfig().endpointHost,
                  stateKeys.at(i),
                  actualIdxValue);
                return 1;
            }
        }
    }

    return 0;
}

void registerSchedulerTestFunctions()
{
    registerDistTestExecutorCallback("threads", "simple", handleSimpleThread);

    registerDistTestExecutorCallback("funcs", "simple", handleSimpleFunction);

    registerDistTestExecutorCallback(
      "snapshots", "fake-diffs", handleFakeDiffsFunction);

    registerDistTestExecutorCallback(
      "snapshots", "fake-diffs-threaded", handleFakeDiffsThreadedFunction);

    registerDistTestExecutorCallback("coord", "barrier", handleSimpleFunction);
}
}
