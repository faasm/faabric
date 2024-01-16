#include "DistTestExecutor.h"

#include <sys/mman.h>

#include <faabric/planner/PlannerClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/snapshot.h>

using namespace faabric::executor;

namespace tests {

static std::unordered_map<std::string, ExecutorFunction> executorFunctions;

void registerDistTestExecutorCallback(const char* user,
                                      const char* funcName,
                                      ExecutorFunction func)
{
    std::string key = std::string(user) + "_" + std::string(funcName);
    executorFunctions[key] = func;

    SPDLOG_DEBUG("Registered executor callback for {}", key);
}

ExecutorFunction getDistTestExecutorCallback(const faabric::Message& msg)
{
    std::string key = msg.user() + "_" + msg.function();
    if (executorFunctions.find(key) == executorFunctions.end()) {

        SPDLOG_ERROR("No registered executor callback for {}", key);
        throw std::runtime_error(
          "Could not find executor callback for function");
    }

    return executorFunctions[key];
}

DistTestExecutor::DistTestExecutor(faabric::Message& msg)
  : Executor(msg)
{
    setUpDummyMemory(dummyMemorySize);
}

DistTestExecutor::~DistTestExecutor() {}

int32_t DistTestExecutor::executeTask(
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    const faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    // Look up function and invoke
    ExecutorFunction callback = getDistTestExecutorCallback(msg);
    return callback(this, threadPoolIdx, msgIdx, req);
}

void DistTestExecutor::reset(faabric::Message& msg)
{
    SPDLOG_DEBUG("Dist test executor resetting for {}",
                 faabric::util::funcToString(msg, false));
}

std::span<uint8_t> DistTestExecutor::getMemoryView()
{
    if (dummyMemory.get() == nullptr) {
        SPDLOG_ERROR("Dist test executor using memory view on null memory");
        throw std::runtime_error("DistTestExecutor null memory");
    }
    return { dummyMemory.get(), dummyMemorySize };
}

void DistTestExecutor::setMemorySize(size_t newSize)
{
    if (newSize != dummyMemorySize) {
        SPDLOG_ERROR("DistTestExecutor cannot change memory size ({} != {})",
                     newSize,
                     dummyMemorySize);
        throw std::runtime_error(
          "DistTestExecutor does not support changing memory size");
    }
}

std::span<uint8_t> DistTestExecutor::getDummyMemory()
{
    return { dummyMemory.get(), dummyMemorySize };
}

void DistTestExecutor::setUpDummyMemory(size_t memSize)
{
    SPDLOG_DEBUG("Dist test executor initialising memory size {}", memSize);
    dummyMemory = faabric::util::allocatePrivateMemory(memSize);
    dummyMemorySize = memSize;
}

std::vector<std::pair<uint32_t, int32_t>> DistTestExecutor::executeThreads(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  const std::vector<faabric::util::SnapshotMergeRegion>& mergeRegions)
{
    SPDLOG_DEBUG("Executor {} executing {} threads", id, req->messages_size());

    std::string funcStr = faabric::util::funcToString(req);
    bool isSingleHost = req->singlehost();

    // Do snapshotting if not on a single host
    faabric::Message& msg = req->mutable_messages()->at(0);
    std::shared_ptr<faabric::util::SnapshotData> snap = nullptr;
    if (!isSingleHost) {
        snap = getMainThreadSnapshot(msg, true);

        // Get dirty regions since last batch of threads
        std::span<uint8_t> memView = getMemoryView();
        tracker->stopTracking(memView);
        tracker->stopThreadLocalTracking(memView);

        // If this is the first batch, these dirty regions will be empty
        std::vector<char> dirtyRegions = tracker->getBothDirtyPages(memView);

        // Apply changes to snapshot
        snap->fillGapsWithBytewiseRegions();
        std::vector<faabric::util::SnapshotDiff> updates =
          snap->diffWithDirtyRegions(memView, dirtyRegions);

        if (updates.empty()) {
            SPDLOG_DEBUG(
              "No updates to main thread snapshot for {} over {} pages",
              faabric::util::funcToString(msg, false),
              dirtyRegions.size());
        } else {
            SPDLOG_DEBUG("Updating main thread snapshot for {} with {} diffs",
                         faabric::util::funcToString(msg, false),
                         updates.size());
            snap->applyDiffs(updates);
        }

        // Clear merge regions, not persisted between batches of threads
        snap->clearMergeRegions();

        // Now we have to add any merge regions we've been saving up for this
        // next batch of threads
        for (const auto& mr : mergeRegions) {
            snap->addMergeRegion(
              mr.offset, mr.length, mr.dataType, mr.operation);
        }
    }

    // Invoke threads and await
    // TODO: for the time being, threads may execute for a long time so we
    // are a bit more generous with the timeout
    auto decision = faabric::planner::getPlannerClient().callFunctions(req);
    auto& sch = faabric::scheduler::getScheduler();
    std::vector<std::pair<uint32_t, int32_t>> results = sch.awaitThreadResults(
      req, 10 * faabric::util::getSystemConfig().boundTimeout);

    // Perform snapshot updates if not on single host
    if (!isSingleHost) {
        // Add the diffs corresponding to this executor
        auto diffs = mergeDirtyRegions(msg);
        snap->queueDiffs(diffs);

        // Write queued changes to snapshot
        int nWritten = snap->writeQueuedDiffs();

        // Remap memory to snapshot if it's been updated
        std::span<uint8_t> memView = getMemoryView();
        if (nWritten > 0) {
            setMemorySize(snap->getSize());
            snap->mapToMemory(memView);
        }

        // Start tracking again
        memView = getMemoryView();
        tracker->startTracking(memView);
        tracker->startThreadLocalTracking(memView);
    }

    return results;
}

std::shared_ptr<Executor> DistTestExecutorFactory::createExecutor(
  faabric::Message& msg)
{
    return std::make_shared<DistTestExecutor>(msg);
}
}
