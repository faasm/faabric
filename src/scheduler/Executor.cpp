#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/dirty.h>
#include <faabric/util/environment.h>
#include <faabric/util/exec_graph.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/queue.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/timing.h>

#define POOL_SHUTDOWN -1

namespace faabric::scheduler {

static thread_local Executor* executingExecutor = nullptr;

Executor* getExecutingExecutor()
{
    return executingExecutor;
}

void setExecutingExecutor(Executor* exec)
{
    executingExecutor = exec;
}

ExecutorTask::ExecutorTask(int messageIndexIn,
                           std::shared_ptr<faabric::BatchExecuteRequest> reqIn,
                           std::shared_ptr<std::atomic<int>> batchCounterIn,
                           bool skipResetIn)
  : req(std::move(reqIn))
  , batchCounter(std::move(batchCounterIn))
  , messageIndex(messageIndexIn)
  , skipReset(skipResetIn)
{}

// TODO - avoid the copy of the message here?
Executor::Executor(faabric::Message& msg)
  : boundMessage(msg)
  , reg(faabric::snapshot::getSnapshotRegistry())
  , tracker(faabric::util::getDirtyTracker())
  , threadPoolSize(faabric::util::getUsableCores())
  , threadPoolThreads(threadPoolSize)
  , threadTaskQueues(threadPoolSize)
{
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();

    assert(!boundMessage.user().empty());
    assert(!boundMessage.function().empty());

    // Set an ID for this Executor
    id = conf.endpointHost + "_" + std::to_string(faabric::util::generateGid());
    SPDLOG_DEBUG("Starting executor {}", id);

    // Mark all thread pool threads as available
    for (int i = 0; i < threadPoolSize; i++) {
        availablePoolThreads.insert(i);
    }
}

void Executor::finish()
{
    SPDLOG_DEBUG("Executor {} shutting down", id);

    // Shut down thread pools and wait
    for (int i = 0; i < threadPoolThreads.size(); i++) {
        // Send a kill message
        SPDLOG_TRACE("Executor {} killing thread pool {}", id, i);
        threadTaskQueues[i].enqueue(
          ExecutorTask(POOL_SHUTDOWN, nullptr, nullptr, false));

        faabric::util::UniqueLock threadsLock(threadsMutex);
        // Copy shared_ptr to avoid racing
        auto thread = threadPoolThreads.at(i);
        // If already killed, move to the next thread
        if (thread == nullptr) {
            continue;
        }

        // Await the thread
        if (thread->joinable()) {
            threadsLock.unlock();
            thread->join();
        }
    }

    // Await dead threads
    for (auto dt : deadThreads) {
        if (dt->joinable()) {
            dt->join();
        }
    }

    // Hook
    this->postFinish();

    // Reset variables
    boundMessage.Clear();

    claimed = false;

    threadPoolThreads.clear();
    threadTaskQueues.clear();
    deadThreads.clear();
}

void Executor::executeTasks(std::vector<int> msgIdxs,
                            std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    const std::string funcStr = faabric::util::funcToString(req);
    SPDLOG_TRACE("{} executing {}/{} tasks of {}",
                 id,
                 msgIdxs.size(),
                 req->messages_size(),
                 funcStr);

    // Note that this lock is specific to this executor, so will only block when
    // multiple threads are trying to schedule tasks.
    // This will only happen when child threads of the same function are
    // competing to schedule more threads, hence is rare so we can afford to be
    // conservative here.
    faabric::util::UniqueLock lock(threadsMutex);

    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    std::string thisHost = faabric::util::getSystemConfig().endpointHost;

    bool isMaster = firstMsg.masterhost() == thisHost;
    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;

    if (isThreads) {
        // Check we get a valid memory view
        std::span<uint8_t> memView = getMemoryView();
        if (memView.empty()) {
            SPDLOG_ERROR("Can't execute threads for {}, empty memory view",
                         funcStr);
            throw std::runtime_error("Empty memory view for threaded function");
        }

        // Restore threads from main thread snapshot
        std::string snapKey = faabric::util::getMainThreadSnapshotKey(firstMsg);
        SPDLOG_DEBUG(
          "Restoring thread of {} from snapshot {}", funcStr, snapKey);
        restore(snapKey);

        // Start global tracking of memory
        tracker.startTracking(memView);
    } else if (!firstMsg.snapshotkey().empty()) {
        // Restore from snapshot if provided
        std::string snapshotKey = firstMsg.snapshotkey();
        SPDLOG_DEBUG("Restoring {} from snapshot {}", funcStr, snapshotKey);
        restore(snapshotKey);
    }

    // Set up shared counter for this batch of tasks
    auto batchCounter = std::make_shared<std::atomic<int>>(msgIdxs.size());

    // Work out if we should skip the reset after this batch. This happens for
    // threads, as they will be restored from their respective snapshot on the
    // next execution.
    bool skipReset = isThreads;

    // Iterate through and invoke tasks. By default, we allocate tasks
    // one-to-one with thread pool threads. Only once the pool is exhausted do
    // we start overloading
    for (int msgIdx : msgIdxs) {
        const faabric::Message& msg = req->messages().at(msgIdx);

        int threadPoolIdx = -1;
        if (availablePoolThreads.empty()) {
            // Here all threads are still executing, so we have to overload.
            // If any tasks are blocking we risk a deadlock, and can no longer
            // guarantee the application will finish.
            // In general if we're on the master host and this is a thread, we
            // should avoid the zeroth and first pool threads as they are likely
            // to be the main thread and the zeroth in the communication group,
            // so will be blocking.
            if (isThreads && isMaster) {
                if (threadPoolSize <= 2) {
                    SPDLOG_ERROR(
                      "Insufficient pool threads ({}) to overload {} idx {}",
                      threadPoolSize,
                      funcStr,
                      msg.appidx());

                    throw std::runtime_error("Insufficient pool threads");
                }

                threadPoolIdx = (msg.appidx() % (threadPoolSize - 2)) + 2;
            } else {
                threadPoolIdx = msg.appidx() % threadPoolSize;
            }

            SPDLOG_DEBUG("Overloaded app index {} to thread {}",
                         msg.appidx(),
                         threadPoolIdx);
        } else {
            // Take next from those that are available
            threadPoolIdx = *availablePoolThreads.begin();
            availablePoolThreads.erase(threadPoolIdx);

            SPDLOG_TRACE("Assigned app index {} to thread {}",
                         msg.appidx(),
                         threadPoolIdx);
        }

        // Enqueue the task
        threadTaskQueues[threadPoolIdx].enqueue(
          ExecutorTask(msgIdx, req, batchCounter, skipReset));

        // Lazily create the thread
        if (threadPoolThreads.at(threadPoolIdx) == nullptr) {
            threadPoolThreads.at(threadPoolIdx) = std::make_shared<std::thread>(
              &Executor::threadPoolThread, this, threadPoolIdx);
        }
    }
}

std::shared_ptr<faabric::util::SnapshotData> Executor::getMainThreadSnapshot(
  faabric::Message& msg)
{
    std::string snapshotKey = faabric::util::getMainThreadSnapshotKey(msg);

    if (!reg.snapshotExists(snapshotKey)) {
        SPDLOG_ERROR(
          "No main thread snapshot for {}, must have threading enabled",
          faabric::util::funcToString(msg, false));
    }

    return reg.getSnapshot(snapshotKey);
}

std::string Executor::createMainThreadSnapshot(const faabric::Message& msg)
{
    std::string snapshotKey = faabric::util::getMainThreadSnapshotKey(msg);

    std::string funcStr = faabric::util::funcToString(msg, false);
    SPDLOG_DEBUG(
      "Creating main thread snapshot: {} for {}", snapshotKey, funcStr);

    std::shared_ptr<faabric::util::SnapshotData> data =
      std::make_shared<faabric::util::SnapshotData>(getMemoryView());
    reg.registerSnapshot(snapshotKey, data);

    return snapshotKey;
}

void Executor::writeChangesToMainThreadSnapshot(faabric::Message& msg)
{
    std::shared_ptr<faabric::util::SnapshotData> snap =
      getMainThreadSnapshot(msg);

    SPDLOG_DEBUG("Updating main thread snapshot for {}",
                 faabric::util::funcToString(msg, false));

    std::span<uint8_t> memView = getMemoryView();

    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);

    std::vector<faabric::util::OffsetMemoryRegion> dirtyRegions =
      tracker.getDirtyOffsets(memView);

    std::vector<faabric::util::SnapshotDiff> updates =
      snap->diffWithDirtyRegions(dirtyRegions);

    snap->queueDiffs(updates);
    snap->writeQueuedDiffs();
}

void Executor::readChangesFromMainThreadSnapshot(faabric::Message& msg)
{
    std::shared_ptr<faabric::util::SnapshotData> snap =
      getMainThreadSnapshot(msg);
    SPDLOG_DEBUG("Reading changes from main thread snapshot for {}",
                 faabric::util::funcToString(msg, false));

    // Set the memory size
    setMemorySize(snap->getSize());

    // Remap the memory
    std::span<uint8_t> memView = getMemoryView();
    snap->mapToMemory(memView);

    // Start tracking again
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);
}

void Executor::deleteMainThreadSnapshot(const faabric::Message& msg)
{
    std::string snapshotKey = faabric::util::getMainThreadSnapshotKey(msg);

    SPDLOG_DEBUG("Deleting main thread snapshot for {}",
                 faabric::util::funcToString(msg, false));

    if (reg.snapshotExists(snapshotKey)) {
        // Broadcast the deletion
        faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
        sch.broadcastSnapshotDelete(msg, snapshotKey);

        // Delete locally
        reg.deleteSnapshot(snapshotKey);
    }
}

void Executor::threadPoolThread(int threadPoolIdx)
{
    SPDLOG_DEBUG("Thread pool thread {}:{} starting up", id, threadPoolIdx);

    auto& sch = faabric::scheduler::getScheduler();
    faabric::transport::PointToPointBroker& broker =
      faabric::transport::getPointToPointBroker();
    const auto& conf = faabric::util::getSystemConfig();

    bool selfShutdown = false;

    for (;;) {
        SPDLOG_TRACE("Thread starting loop {}:{}", id, threadPoolIdx);

        ExecutorTask task;

        try {
            task = threadTaskQueues[threadPoolIdx].dequeue(conf.boundTimeout);
        } catch (faabric::util::QueueTimeoutException& ex) {
            // If the thread has had no messages, it needs to remove itself
            SPDLOG_TRACE("Thread {}:{} got no messages in timeout {}ms",
                         id,
                         threadPoolIdx,
                         conf.boundTimeout);
            selfShutdown = true;
            break;
        }

        // If the thread is being killed, the executor itself
        // will handle the clean-up
        if (task.messageIndex == POOL_SHUTDOWN) {
            SPDLOG_DEBUG("Killing thread pool thread {}:{}", id, threadPoolIdx);
            selfShutdown = false;
            break;
        }

        assert(task.req->messages_size() >= task.messageIndex + 1);
        faabric::Message& msg =
          task.req->mutable_messages()->at(task.messageIndex);

        // If the incoming message has threading set, we know this is the
        // top-level main task for a threaded application
        bool isMainThread = false;
        bool isThreads =
          task.req->type() == faabric::BatchExecuteRequest::THREADS;
        if (msg.isthreaded()) {
            isMainThread = true;

            // Start tracking main thread memory
            std::span<uint8_t> memView = getMemoryView();
            tracker.startTracking(memView);
            tracker.startThreadLocalTracking(memView);

            // Set up the main snapshot for the threaded application
            createMainThreadSnapshot(msg);
        }

        // Start dirty tracking if necessary
        if (isThreads) {
            // If tracking is thread local, start here as it will happen for
            // each thread
            tracker.startThreadLocalTracking(getMemoryView());
        }

        // Check ptp group
        std::shared_ptr<faabric::transport::PointToPointGroup> group = nullptr;
        if (msg.groupid() > 0) {
            group =
              faabric::transport::PointToPointGroup::getGroup(msg.groupid());
        }

        bool isMaster = msg.masterhost() == conf.endpointHost;
        SPDLOG_TRACE("Thread {}:{} executing task {} ({}, thread={}, group={})",
                     id,
                     threadPoolIdx,
                     task.messageIndex,
                     msg.id(),
                     isThreads,
                     msg.groupid());

        // Set executing executor
        setExecutingExecutor(this);

        // Execute the task
        int32_t returnValue;
        try {
            returnValue =
              executeTask(threadPoolIdx, task.messageIndex, task.req);
        } catch (const std::exception& ex) {
            returnValue = 1;

            std::string errorMessage = fmt::format(
              "Task {} threw exception. What: {}", msg.id(), ex.what());
            SPDLOG_ERROR(errorMessage);
            msg.set_outputdata(errorMessage);
        }

        // Handle thread-local diffing for every thread
        if (isThreads) {
            std::span<uint8_t> memView = getMemoryView();
            auto thisThreadDirtyRegions =
              tracker.getThreadLocalDirtyOffsets(memView);

            // Add to executor-wide list of dirty regions
            faabric::util::FullLock lock(dirtyRegionsMutex);
            dirtyRegions.insert(dirtyRegions.end(),
                                thisThreadDirtyRegions.begin(),
                                thisThreadDirtyRegions.end());
        }

        // Set the return value
        msg.set_returnvalue(returnValue);

        // Decrement the task count
        std::atomic_thread_fence(std::memory_order_release);
        int oldTaskCount = task.batchCounter->fetch_sub(1);
        assert(oldTaskCount >= 0);
        bool isLastInBatch = oldTaskCount == 1;

        SPDLOG_TRACE("Task {} finished by thread {}:{} ({} left)",
                     faabric::util::funcToString(msg, true),
                     id,
                     threadPoolIdx,
                     oldTaskCount - 1);

        // Handle snapshot diffs _before_ we reset the executor
        if (isLastInBatch && isThreads) {
            // Stop non-thread-local tracking as we're the last in the batch
            std::span<uint8_t> memView = getMemoryView();
            tracker.stopTracking(memView);

            // Add non-thread-local dirty regions
            {
                faabric::util::FullLock lock(dirtyRegionsMutex);
                std::vector<faabric::util::OffsetMemoryRegion> r =
                  tracker.getDirtyOffsets(memView);

                dirtyRegions.insert(dirtyRegions.end(), r.begin(), r.end());
            }

            // Fill snapshot gaps with overwrite regions first
            std::string mainThreadSnapKey =
              faabric::util::getMainThreadSnapshotKey(msg);
            auto snap = reg.getSnapshot(mainThreadSnapKey);
            snap->fillGapsWithOverwriteRegions();

            // Compare snapshot with all dirty regions for this executor
            std::vector<faabric::util::SnapshotDiff> diffs;
            {
                // Do the diffing
                faabric::util::FullLock lock(dirtyRegionsMutex);
                diffs = snap->diffWithDirtyRegions(dirtyRegions);
                dirtyRegions.clear();
            }

            SPDLOG_DEBUG("Queueing {} diffs for {} to snapshot {} (group {})",
                         diffs.size(),
                         faabric::util::funcToString(msg, false),
                         mainThreadSnapKey,
                         msg.groupid());

            // On master we queue the diffs locally directly, on a remote
            // host we push them back to master
            if (isMaster) {
                snap->queueDiffs(diffs);
            } else if (isLastInBatch) {
                sch.pushSnapshotDiffs(msg, mainThreadSnapKey, diffs);
            }

            // If last in batch on this host, clear the merge regions
            SPDLOG_DEBUG("Clearing merge regions for {}", mainThreadSnapKey);
            snap->clearMergeRegions();
        }

        // If this is the last in the app itself, delete the main thread
        // snapshot, and stop tracking
        if (isLastInBatch && isMainThread) {
            // Stop tracking main thread memory
            std::span<uint8_t> memView = getMemoryView();
            tracker.stopTracking(memView);
            tracker.stopThreadLocalTracking(memView);

            // Delete the main thread snapshot
            deleteMainThreadSnapshot(msg);
        }

        // If this batch is finished, reset the executor and release its
        // claim. Note that we have to release the claim _after_ resetting,
        // otherwise the executor won't be ready for reuse.
        if (isLastInBatch) {
            if (task.skipReset) {
                SPDLOG_TRACE("Skipping reset for {}",
                             faabric::util::funcToString(msg, true));
            } else {
                reset(msg);
            }

            releaseClaim();
        }

        // Return this thread index to the pool available for scheduling
        {
            faabric::util::UniqueLock lock(threadsMutex);
            availablePoolThreads.insert(threadPoolIdx);
        }

        // Vacate the slot occupied by this task. This must be done after
        // releasing the claim on this executor, otherwise the scheduler may
        // try to schedule another function and be unable to reuse this
        // executor.
        sch.vacateSlot();

        // Finally set the result of the task, this will allow anything
        // waiting on its result to continue execution, therefore must be
        // done once the executor has been reset, otherwise the executor may
        // not be reused for a repeat invocation.
        if (isThreads) {
            // Set non-final thread result
            sch.setThreadResult(msg, returnValue);
        } else {
            // Set normal function result
            sch.setFunctionResult(msg);
        }
    }

    if (selfShutdown) {
        SPDLOG_DEBUG(
          "Shutting down thread pool thread {}:{}", id, threadPoolIdx);

        // Note - we have to keep a record of dead threads so we can join
        // them all when the executor shuts down
        bool isFinished = true;
        {
            faabric::util::UniqueLock threadsLock(threadsMutex);
            std::shared_ptr<std::thread> thisThread =
              threadPoolThreads.at(threadPoolIdx);
            deadThreads.emplace_back(thisThread);

            // Make sure we're definitely not still tracking changes
            tracker.stopTracking(getMemoryView());

            // Set this thread to nullptr
            threadPoolThreads.at(threadPoolIdx) = nullptr;

            // See if any threads are still running
            for (auto t : threadPoolThreads) {
                if (t != nullptr) {
                    isFinished = false;
                    break;
                }
            }
        }

        if (isFinished) {
            // Notify that this executor is finished
            sch.notifyExecutorShutdown(this, boundMessage);
        }
    }

    // We have to clean up TLS here as this should be the last use of the
    // scheduler and point-to-point broker from this thread
    sch.resetThreadLocalCache();
    broker.resetThreadLocalCache();
}

bool Executor::tryClaim()
{
    bool expected = false;
    bool wasClaimed = claimed.compare_exchange_strong(expected, true);
    return wasClaimed;
}

void Executor::releaseClaim()
{
    claimed.store(false);
}

// ------------------------------------------
// HOOKS
// ------------------------------------------

int32_t Executor::executeTask(int threadPoolIdx,
                              int msgIdx,
                              std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return 0;
}

void Executor::postFinish() {}

void Executor::reset(faabric::Message& msg) {}

std::span<uint8_t> Executor::getMemoryView()
{
    SPDLOG_WARN("Executor for {} has not implemented memory view method",
                faabric::util::funcToString(boundMessage, false));
    return {};
}

void Executor::setMemorySize(size_t newSize)
{
    SPDLOG_WARN("Executor has not implemented set memory size method");
}

void Executor::restore(const std::string& snapshotKey)
{
    SPDLOG_WARN("Executor has not implemented restore method");
}
}
