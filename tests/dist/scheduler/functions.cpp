#include <catch2/catch.hpp>

#include "DistTestExecutor.h"
#include "faabric_utils.h"
#include "init.h"

#include <sys/mman.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/bytes.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

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

    std::string msgInput = msg.inputdata();
    std::string snapshotKey = msg.snapshotkey();

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    auto originalSnap = reg.getSnapshot(snapshotKey);
    std::shared_ptr<faabric::util::SnapshotData> updatedSnap = exec->snapshot();

    // Add a single merge region to catch both diffs
    int offsetA = 10;
    int offsetB = 100;
    std::vector<uint8_t> inputBytes = faabric::util::stringToBytes(msgInput);

    originalSnap->addMergeRegion(
      0,
      offsetB + inputBytes.size() + 10,
      faabric::util::SnapshotDataType::Raw,
      faabric::util::SnapshotMergeOperation::Overwrite);

    // Modify the executor's memory
    std::vector<uint8_t> keyBytes = faabric::util::stringToBytes(snapshotKey);
    updatedSnap->copyInData(keyBytes, offsetA);
    updatedSnap->copyInData(inputBytes, offsetB);

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

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    // This function creates a snapshot, then spawns some child threads that
    // will modify the shared memory. It then awaits the results and checks that
    // the modifications are synced back to the original host.
    if (!isThread) {
        int nThreads = std::stoi(msgInput);

        // Set up the snapshot
        size_t snapSize = (nThreads * 4) * faabric::util::HOST_PAGE_SIZE;
        auto snap = std::make_shared<faabric::util::SnapshotData>(snapSize);
        reg.registerSnapshot(snapshotKey, snap);

        auto req =
          faabric::util::batchExecFactory(msg.user(), msg.function(), nThreads);
        req->set_type(faabric::BatchExecuteRequest::THREADS);

        for (int i = 0; i < nThreads; i++) {
            auto& m = req->mutable_messages()->at(i);
            m.set_appidx(i);
            m.set_inputdata(std::string("thread_" + std::to_string(i)));
            m.set_snapshotkey(snapshotKey);

            // Make a small modification to a page that will also be edited by
            // the child thread to make sure it's not overwritten
            std::vector<uint8_t> localChange(3, i);
            int offset = 2 * i * faabric::util::HOST_PAGE_SIZE;
            snap->copyInData(localChange, offset);
        }

        // Dispatch the message, expecting them all to execute on other hosts
        std::string thisHost = faabric::util::getSystemConfig().endpointHost;
        faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();

        std::vector<std::string> executedHosts = sch.callFunctions(req).hosts;

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
            int localOffset = 2 * i * faabric::util::HOST_PAGE_SIZE;
            std::vector<uint8_t> actualLocal =
              snap->getDataCopy(localOffset, expectedLocal.size());

            if (actualLocal != expectedLocal) {
                SPDLOG_ERROR("Local modifications not present for {}", i);
                success = false;
            }

            // Check remote modifications
            int offset = 2 * i * faabric::util::HOST_PAGE_SIZE + 10;
            std::string expectedData("thread_" + std::to_string(i));
            auto* charPtr =
              reinterpret_cast<char*>(snap->getMutableDataPtr(offset));
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
        // This is the code that will be executed by the remote threads.
        // Add a merge region to catch the modification
        int idx = msg.appidx();

        int regionOffset = 2 * idx * faabric::util::HOST_PAGE_SIZE;
        int changeOffset = regionOffset + 10;

        // Get the input data
        std::vector<uint8_t> inputBytes =
          faabric::util::stringToBytes(msgInput);

        auto originalSnap = reg.getSnapshot(snapshotKey);
        std::shared_ptr<faabric::util::SnapshotData> updatedSnap =
          exec->snapshot();

        // Make sure it's captured by the region
        int regionLength = 20 + inputBytes.size();
        originalSnap->addMergeRegion(
          regionOffset,
          regionLength,
          faabric::util::SnapshotDataType::Raw,
          faabric::util::SnapshotMergeOperation::Overwrite);

        // Now modify the memory
        updatedSnap->copyInData(inputBytes, changeOffset);

        return 0;
    }

    return 333;
}

void registerSchedulerTestFunctions()
{
    registerDistTestExecutorCallback("threads", "simple", handleSimpleThread);

    registerDistTestExecutorCallback("funcs", "simple", handleSimpleFunction);

    registerDistTestExecutorCallback(
      "snapshots", "fake-diffs", handleFakeDiffsFunction);

    registerDistTestExecutorCallback(
      "snapshots", "fake-diffs-threaded", handleFakeDiffsThreadedFunction);
}
}
