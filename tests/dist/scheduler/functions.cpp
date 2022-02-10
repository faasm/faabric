#include <catch2/catch.hpp>

#include "DistTestExecutor.h"
#include "faabric/snapshot/SnapshotRegistry.h"
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

/**
 * This function is aimed at checking the single host optimisations and thread
 * scheduling performed by an executor. On a single host we expect no dirty
 * page tracking or diff merging to be performed. On multiple hosts, the
 * executor should transparently create the main thread snapshot, update it from
 * changes in the main thread, and sync those changes with remote hosts.
 */
int handleThreadScheduling(tests::DistTestExecutor* exec,
                           int threadPoolIdx,
                           int msgIdx,
                           std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);
    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    std::string snapshotKey = faabric::util::getMainThreadSnapshotKey(msg);

    std::span<uint8_t> memView = exec->getMemoryView();

    bool isThread = req->type() == faabric::BatchExecuteRequest::THREADS;

    int nThreads = 5;
    int nLoops = 5;

    std::vector<SnapshotMergeRegion> mergeRegions;

    if (!isThread) {
        bool isSingleHost = false;

        // This is the main function, so need to spawn threads in a loop
        for (int loop = 0; loop < nLoops; loop++) {
            // Set up the request
            auto req = faabric::util::batchExecFactory(
              msg.user(), msg.function(), nThreads);
            req->set_type(faabric::BatchExecuteRequest::THREADS);

            // Spawn and await the threads
            std::vector<std::pair<uint32_t, int32_t>> results =
              exec->executeThreads(req, mergeRegions);

            // Check the results
            if (results.size() != nThreads) {
                SPDLOG_ERROR("Thread results not expected length: {} != {}",
                             results.size(),
                             nThreads);
                return 1;
            }

            for (int i = 0; i < nThreads; i++) {
                const faabric::Message& msg = req->messages().at(i);
                uint32_t msgId = results[i].first;
                int32_t res = results[i].second;

                if (msgId != msg.id()) {
                    SPDLOG_ERROR(
                      "Thread result msg id {} not as expected: {} != {}",
                      i,
                      msgId,
                      msg.id());
                    return 1;
                }
                if (res != 0) {
                    SPDLOG_ERROR("Thread {} id {} failed: {}", i, msgId, res);
                    return 1;
                }
            }

            // Check whether snapshot has been updated (expect so on multi-host,
            // but not on single)
            isSingleHost = req->singlehost();
            if (isSingleHost) {
                if (reg.snapshotExists(snapshotKey)) {
                    SPDLOG_ERROR(
                      "Executed on single host but snapshot {} exists",
                      snapshotKey);
                    return 1;
                }
            } else {
                if (!reg.snapshotExists(snapshotKey)) {
                    SPDLOG_ERROR("Executed on multiple hosts but snapshot {} "
                                 "does not exist",
                                 snapshotKey);
                    return 1;
                }
            }

            // Check memory has been updated by all threads
        }

    } else {
        if (req->singlehost()) {
            // Check snapshot has not been created
            if (reg.snapshotExists(snapshotKey)) {
                SPDLOG_ERROR("Thread on single host but snapshot {} exists",
                             snapshotKey);
                return 1;
            }
        } else {
            // Check snapshot has been created
            if (reg.snapshotExists(snapshotKey)) {
                SPDLOG_ERROR(
                  "Thread on remote host but snapshot {} does not exist",
                  snapshotKey);
                return 1;
            }
        }

        // Work out which iteration and thread we are
        int iteration = std::stoi(msg.inputdata());
        int threadIdx = msg.appidx();
        int offset = threadIdx * HOST_PAGE_SIZE;

        // Check value has edits from the main thread
        int expectedValue = (iteration * threadIdx) + iteration;
        int value = unalignedRead<int>(memView.data() + offset);

        if(value != expectedValue) {
            SPDLOG_ERROR("Thread {} got unexpected value on iteration {}. {} != {}",
                    threadIdx, iteration, value, expectedValue);

            return 1;
        }

        // Increment the value for this thread
        int newValue = value + threadIdx;

        unalignedWrite<int>(newValue, memView.data() + offset);
    }

    return 0;
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

        auto req =
          faabric::util::batchExecFactory(msg.user(), msg.function(), nThreads);
        req->set_type(faabric::BatchExecuteRequest::THREADS);

        for (int i = 0; i < nThreads; i++) {
            auto& m = req->mutable_messages()->at(i);
            m.set_appid(msg.appid());
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

        // Dispatch the message
        std::vector<std::pair<uint32_t, int32_t>> results =
          exec->executeThreads(req, {});

        // Check results
        for (auto [mid, res] : results) {
            if (res != 0) {
                SPDLOG_ERROR(
                  "Thread diffs test thread {} failed with value {}", mid, res);
                throw std::runtime_error("Thread diffs check failed");
            }
        }

        // Check changes have been applied
        auto snap = exec->getMainThreadSnapshot(msg);
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

        if (regionOffset > exec->getDummyMemory().size()) {
            SPDLOG_ERROR(
              "Dummy memory not large enough for function {} ({} > {})",
              faabric::util::funcToString(msg, false),
              regionOffset,
              exec->getDummyMemory().size());
            throw std::runtime_error("Dummy memory not large enough");
        }

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
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    int nThreads = 4;
    int nRepeats = 20;

    // Perform two reductions and one array modification. One reduction on same
    // page as array change
    uint32_t reductionAOffset = HOST_PAGE_SIZE;
    uint32_t reductionBOffset = 2 * HOST_PAGE_SIZE;
    uint32_t arrayOffset = HOST_PAGE_SIZE + 10 * sizeof(int32_t);

    bool isThread = req->type() == faabric::BatchExecuteRequest::THREADS;

    // Main function will set up the snapshot and merge regions, while the child
    // threads will modify an array and perform a reduction operation
    if (!isThread) {
        // Get the main thread snapshot, creating if doesn't already exist
        auto snap = exec->getMainThreadSnapshot(msg, true);

        // Perform operations in a loop
        for (int r = 0; r < nRepeats; r++) {
            // Set up thread request
            auto req = faabric::util::batchExecFactory(
              msg.user(), msg.function(), nThreads);
            req->set_type(faabric::BatchExecuteRequest::THREADS);
            for (int i = 0; i < nThreads; i++) {
                auto& m = req->mutable_messages()->at(i);

                // Set app/ group info
                m.set_appid(msg.appid());
                m.set_appidx(i);
                m.set_groupidx(i);
            }

            // Set merge regions
            std::vector<faabric::util::SnapshotMergeRegion> mergeRegions = {
                { reductionAOffset,
                  sizeof(int32_t),
                  SnapshotDataType::Int,
                  SnapshotMergeOperation::Sum },

                { reductionBOffset,
                  sizeof(int32_t),
                  SnapshotDataType::Int,
                  SnapshotMergeOperation::Sum }
            };

            // Execute the threads
            std::vector<std::pair<uint32_t, int32_t>> results =
              exec->executeThreads(req, mergeRegions);

            // Check thread results
            for (auto [mid, res] : results) {
                if (res != 0) {
                    SPDLOG_ERROR(
                      "Distributed reduction test thread {} failed: {}",
                      mid,
                      res);

                    return 1;
                }
            }

            SPDLOG_DEBUG("Reduce test threads finished");

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
        int groupId = msg.groupid();
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

    registerDistTestExecutorCallback(
      "threads", "sched", handleThreadScheduling);

    registerDistTestExecutorCallback("funcs", "simple", handleSimpleFunction);

    registerDistTestExecutorCallback(
      "snapshots", "fake-diffs", handleFakeDiffsFunction);

    registerDistTestExecutorCallback(
      "snapshots", "fake-diffs-threaded", handleFakeDiffsThreadedFunction);

    registerDistTestExecutorCallback(
      "snapshots", "reduction", handleReductionFunction);
}
}
