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

ExecutorTask::ExecutorTask(int messageIndexIn,
                           std::shared_ptr<faabric::BatchExecuteRequest> reqIn,
                           std::shared_ptr<std::atomic<int>> batchCounterIn,
                           bool needsSnapshotSyncIn,
                           bool skipResetIn)
  : req(std::move(reqIn))
  , batchCounter(std::move(batchCounterIn))
  , messageIndex(messageIndexIn)
  , needsSnapshotSync(needsSnapshotSyncIn)
  , skipReset(skipResetIn)
{}

// TODO - avoid the copy of the message here?
Executor::Executor(faabric::Message& msg)
  : boundMessage(msg)
  , reg(faabric::snapshot::getSnapshotRegistry())
  , tracker(faabric::util::getDirtyPageTracker())
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
          ExecutorTask(POOL_SHUTDOWN, nullptr, nullptr, false, false));

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
    // competing, hence is rare so we can afford to be conservative here.
    faabric::util::UniqueLock lock(threadsMutex);

    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    std::string snapshotKey = firstMsg.snapshotkey();
    std::string thisHost = faabric::util::getSystemConfig().endpointHost;

    bool isMaster = firstMsg.masterhost() == thisHost;
    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;
    bool isSnapshot = !snapshotKey.empty();

    // Restore if we have a snapshot
    if (isSnapshot) {
        SPDLOG_DEBUG("Restoring {} from snapshot {}", funcStr, snapshotKey);
        restore(firstMsg);
    }

    // Reset dirty page tracking if we're executing threads.
    // Note this must be done after the restore has happened.
    bool needsSnapshotSync = false;
    if (isThreads && isSnapshot) {
        needsSnapshotSync = true;

        // If tracking is not thread local, start once before executing tasks
        if (!tracker.isThreadLocal()) {
            tracker.startTracking(getMemoryView());
        }
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
        threadTaskQueues[threadPoolIdx].enqueue(ExecutorTask(
          msgIdx, req, batchCounter, needsSnapshotSync, skipReset));

        // Lazily create the thread
        if (threadPoolThreads.at(threadPoolIdx) == nullptr) {
            threadPoolThreads.at(threadPoolIdx) = std::make_shared<std::thread>(
              &Executor::threadPoolThread, this, threadPoolIdx);
        }
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
            // If the thread has had no messages, it needs to
            // remove itself
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

        // Check ptp group
        std::shared_ptr<faabric::transport::PointToPointGroup> group = nullptr;
        if (msg.groupid() > 0) {
            group =
              faabric::transport::PointToPointGroup::getGroup(msg.groupid());
        }

        // Start dirty tracking if necessary
        std::shared_ptr<faabric::util::SnapshotData> snap = nullptr;
        if (task.needsSnapshotSync) {
            snap = reg.getSnapshot(msg.snapshotkey());

            // If tracking is thread local, start here as it will happen for
            // each thread
            if (tracker.isThreadLocal()) {
                tracker.startTracking(getMemoryView());
            }
        }

        bool isMaster = msg.masterhost() == conf.endpointHost;
        bool isThreads =
          task.req->type() == faabric::BatchExecuteRequest::THREADS;
        SPDLOG_TRACE("Thread {}:{} executing task {} ({}, thread={}, group={})",
                     id,
                     threadPoolIdx,
                     task.messageIndex,
                     msg.id(),
                     isThreads,
                     msg.groupid());

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
        std::span<uint8_t> funcMemory = getMemoryView();
        if (!funcMemory.empty() && task.needsSnapshotSync &&
            tracker.isThreadLocal()) {

            tracker.stopTracking(getMemoryView());
            auto thisThreadDirtyRegions =
              tracker.getDirtyOffsets(getMemoryView());

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
        if (!funcMemory.empty() && isLastInBatch && task.needsSnapshotSync) {
            // If not thread local, we need to diff the memory once here
            if (!tracker.isThreadLocal()) {
                tracker.stopTracking(funcMemory);

                faabric::util::FullLock lock(dirtyRegionsMutex);
                dirtyRegions = tracker.getDirtyOffsets(funcMemory);
            }

            // Fill snapshot gaps with overwrite regions first
            snap->fillGapsWithOverwriteRegions();

            // Compare snapshot with all dirty regions for this executor
            std::vector<faabric::util::SnapshotDiff> diffs;
            {
                // Do the diffing
                faabric::util::SharedLock lock(dirtyRegionsMutex);
                diffs = snap->diffWithDirtyRegions(dirtyRegions);
            }

            SPDLOG_DEBUG("Queueing {} diffs for {} to snapshot {} (group {})",
                         diffs.size(),
                         faabric::util::funcToString(msg, false),
                         msg.snapshotkey(),
                         msg.groupid());

            // On master we queue the diffs locally directly, on a remote
            // host we push them back to master
            if (isMaster) {
                snap->queueDiffs(diffs);
            } else if (isLastInBatch) {
                sch.pushSnapshotDiffs(msg, diffs);
            }

            // If last in batch on this host, clear the merge regions
            SPDLOG_DEBUG("Clearing merge regions for {}", msg.snapshotkey());
            snap->clearMergeRegions();

            {
                faabric::util::FullLock lock(dirtyRegionsMutex);
                dirtyRegions.clear();
            }
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

void Executor::restore(faabric::Message& msg)
{
    SPDLOG_WARN("Executor has not implemented restore method");
}
}
