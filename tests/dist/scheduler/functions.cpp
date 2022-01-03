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
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

using namespace faabric::util;

namespace tests {

int handleSimpleThread(tests::DistTestExecutor* exec,
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

int handleSimpleFunction(tests::DistTestExecutor* exec,
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

int handleFakeDiffsFunction(tests::DistTestExecutor* exec,
                            int threadPoolIdx,
                            int msgIdx,
                            std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    std::string msgInput = msg.inputdata();
    std::vector<uint8_t> inputBytes = faabric::util::stringToBytes(msgInput);
    std::vector<uint8_t> otherData = { 1, 2, 3, 4 };

    // Modify the executor's memory
    int offsetA = 10;
    int offsetB = HOST_PAGE_SIZE + 10;
    std::memcpy(exec->getDummyMemory().data() + offsetA,
                otherData.data(),
                otherData.size());
    std::memcpy(exec->getDummyMemory().data() + offsetB,
                inputBytes.data(),
                inputBytes.size());

    return 123;
}

int handleFakeDiffsThreadedFunction(
  tests::DistTestExecutor* exec,
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    bool isThread = req->type() == faabric::BatchExecuteRequest::THREADS;
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);
    std::string snapshotKey = getMainThreadSnapshotKey(msg);
    std::string msgInput = msg.inputdata();

    // This function spawns some child threads that will modify the shared
    // memory. It then awaits the results and checks that the modifications are
    // synced back to the original host.
    if (!isThread) {
        int nThreads = std::stoi(msgInput);
        size_t snapSize = (nThreads * 4) * faabric::util::HOST_PAGE_SIZE;
        exec->setUpDummyMemory(snapSize);

        auto req =
          faabric::util::batchExecFactory(msg.user(), msg.function(), nThreads);
        req->set_type(faabric::BatchExecuteRequest::THREADS);

        for (int i = 0; i < nThreads; i++) {
            auto& m = req->mutable_messages()->at(i);
            m.set_appidx(i);
            m.set_inputdata(std::string("thread_" + std::to_string(i)));

            // Make a small modification to a page that will also be edited by
            // the child thread to make sure it's not overwritten
            std::vector<uint8_t> localChange(3, i);
            int offset = 2 * i * faabric::util::HOST_PAGE_SIZE;
            std::memcpy(exec->getDummyMemory().data() + offset,
                        localChange.data(),
                        localChange.size());
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

        // Write queued diffs from all the threads
        auto snap = exec->getMainThreadSnapshot(msg);
        snap->writeQueuedDiffs();

        // Check changes have been applied
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
            const auto* charPtr =
              reinterpret_cast<const char*>(snap->getDataPtr(offset));
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
        int idx = msg.appidx();

        int regionOffset = 2 * idx * faabric::util::HOST_PAGE_SIZE;
        int changeOffset = regionOffset + 10;

        // Get the input data
        std::vector<uint8_t> inputBytes =
          faabric::util::stringToBytes(msgInput);

        // Now modify the memory
        std::memcpy(exec->getDummyMemory().data() + changeOffset,
                    inputBytes.data(),
                    inputBytes.size());

        return 0;
    }

    return 333;
}

/*
 * This function performs two reductions and non-conflicting updates to a shared
 * array in a loop to check distributed snapshot synchronisation and merge
 * strategies.
 */
int handleReductionFunction(tests::DistTestExecutor* exec,
                            int threadPoolIdx,
                            int msgIdx,
                            std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    size_t snapSize = 4 * HOST_PAGE_SIZE;
    exec->setUpDummyMemory(snapSize);

    faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    int nThreads = 4;
    int nRepeats = 20;
    int groupId = 1234;

    // Perform two reductions and one array modification. One reduction on same
    // page as array change
    uint32_t reductionAOffset = HOST_PAGE_SIZE;
    uint32_t reductionBOffset = 2 * HOST_PAGE_SIZE;
    uint32_t arrayOffset = HOST_PAGE_SIZE + 10 * sizeof(int32_t);

    bool isThread = req->type() == faabric::BatchExecuteRequest::THREADS;

    // Main function will set up the snapshot and merge regions, while the child
    // threads will modify an array and perform a reduction operation
    if (!isThread) {
        std::string snapKey = faabric::util::getMainThreadSnapshotKey(msg);
        faabric::snapshot::SnapshotRegistry& reg =
          faabric::snapshot::getSnapshotRegistry();

        // Set up snapshot
        std::shared_ptr<SnapshotData> snap = reg.getSnapshot(snapKey);

        // Perform operations in a loop
        for (int r = 0; r < nRepeats; r++) {
            // Set up thread request
            auto req = faabric::util::batchExecFactory(
              msg.user(), msg.function(), nThreads);
            req->set_type(faabric::BatchExecuteRequest::THREADS);
            for (int i = 0; i < nThreads; i++) {
                auto& m = req->mutable_messages()->at(i);

                // Set app/ group info
                m.set_groupid(groupId);
                m.set_groupidx(i);
                m.set_appidx(i);
            }

            // Set merge regions
            snap->addMergeRegion(reductionAOffset,
                                 sizeof(int32_t),
                                 SnapshotDataType::Int,
                                 SnapshotMergeOperation::Sum,
                                 true);

            snap->addMergeRegion(reductionBOffset,
                                 sizeof(int32_t),
                                 SnapshotDataType::Int,
                                 SnapshotMergeOperation::Sum,
                                 true);

            // Make the request
            faabric::scheduler::Scheduler& sch =
              faabric::scheduler::getScheduler();
            std::vector<std::string> actualHosts = sch.callFunctions(req).hosts;

            // Check hosts
            std::string thisHost = getSystemConfig().endpointHost;
            int nThisHost = 0;
            int nOtherHost = 0;
            for (const auto& h : actualHosts) {
                if (h == thisHost) {
                    nThisHost++;
                } else {
                    nOtherHost++;
                }
            }

            if (nThisHost != 2 || nOtherHost != 2) {
                SPDLOG_ERROR("Threads not scheduled as expected: {} {}",
                             nThisHost,
                             nOtherHost);
                return 1;
            }

            // Wait for the threads
            for (const auto& m : req->messages()) {
                int32_t thisRes = sch.awaitThreadResult(m.id());
                if (thisRes != 0) {
                    SPDLOG_ERROR(
                      "Distributed reduction test thread {} failed: {}",
                      m.id(),
                      thisRes);

                    return 1;
                }
            }

            SPDLOG_DEBUG("Reduce test threads finished");

            // Write queued snapshot diffs
            snap->writeQueuedDiffs();

            // Read changes into memory
            exec->readChangesFromMainThreadSnapshot(msg);

            uint8_t* reductionAPtr =
              exec->getDummyMemory().data() + reductionAOffset;
            uint8_t* reductionBPtr =
              exec->getDummyMemory().data() + reductionBOffset;
            uint8_t* arrayPtr = exec->getDummyMemory().data() + arrayOffset;

            // Check everything as expected
            int expectedReductionA = (r + 1) * nThreads * 10;
            int expectedReductionB = (r + 1) * nThreads * 20;
            auto actualReductionA = unalignedRead<int32_t>(reductionAPtr);
            auto actualReductionB = unalignedRead<int32_t>(reductionBPtr);

            bool success = true;

            for (int i = 0; i < nThreads; i++) {
                uint8_t* thisPtr = arrayPtr + (i * sizeof(int32_t));
                int expectedValue = i * 30;
                auto actualValue = unalignedRead<int32_t>(thisPtr);

                if (expectedValue != actualValue) {
                    success = false;
                    SPDLOG_ERROR("Dist array merge at {} failed: {} != {}",
                                 i,
                                 expectedValue,
                                 actualValue);
                }
            }

            if (expectedReductionA != actualReductionA) {
                success = false;
                SPDLOG_ERROR("Dist reduction A failed: {} != {}",
                             expectedReductionA,
                             actualReductionA);
            }

            if (expectedReductionB != actualReductionB) {
                success = false;
                SPDLOG_ERROR("Dist reduction B failed: {} != {}",
                             expectedReductionB,
                             actualReductionB);
            }

            if (!success) {
                return 1;
            }
        }
    } else {
        uint8_t* reductionAPtr =
          exec->getDummyMemory().data() + reductionAOffset;
        uint8_t* reductionBPtr =
          exec->getDummyMemory().data() + reductionBOffset;

        uint8_t* arrayPtr = exec->getDummyMemory().data() + arrayOffset;
        uint32_t thisIdx = msg.appidx();
        uint8_t* thisArrayPtr = arrayPtr + (sizeof(int32_t) * thisIdx);

        // Lock group locally while doing reduction
        std::shared_ptr<faabric::transport::PointToPointGroup> group =
          faabric::transport::PointToPointGroup::getGroup(groupId);
        group->localLock();

        // Make modifications
        int32_t initialA = unalignedRead<int32_t>(reductionAPtr);
        int32_t initialB = unalignedRead<int32_t>(reductionBPtr);

        unalignedWrite(initialA + 10, reductionAPtr);
        unalignedWrite(initialB + 20, reductionBPtr);

        int arrayValue = thisIdx * 30;
        unalignedWrite<int32_t>(arrayValue, thisArrayPtr);

        SPDLOG_DEBUG("Reduce test thread {}: {} {} {}",
                     thisIdx,
                     arrayValue,
                     initialA,
                     initialB);

        // Unlock group
        group->localUnlock();
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

    registerDistTestExecutorCallback(
      "snapshots", "reduction", handleReductionFunction);
}
}
