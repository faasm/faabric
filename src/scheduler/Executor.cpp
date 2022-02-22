#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/ExecutorContext.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
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
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/queue.h>
#include <faabric/util/scheduling.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/string_tools.h>
#include <faabric/util/timing.h>

// Default snapshot size here is set to support 32-bit WebAssembly, but could be
// made configurable on the function call or language.
#define ONE_MB (1024L * 1024L)
#define ONE_GB (1024L * ONE_MB)
#define DEFAULT_MAX_SNAP_SIZE (4 * ONE_GB)

#define POOL_SHUTDOWN -1

namespace faabric::scheduler {

ExecutorTask::ExecutorTask(int messageIndexIn,
                           std::shared_ptr<faabric::BatchExecuteRequest> reqIn,
                           std::shared_ptr<std::atomic<int>> batchCounterIn)
  : req(std::move(reqIn))
  , batchCounter(std::move(batchCounterIn))
  , messageIndex(messageIndexIn)
{}

// TODO - avoid the copy of the message here?
Executor::Executor(faabric::Message& msg)
  : boundMessage(msg)
  , sch(getScheduler())
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
          ExecutorTask(POOL_SHUTDOWN, nullptr, nullptr));

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

std::vector<std::pair<uint32_t, int32_t>> Executor::executeThreads(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  const std::vector<faabric::util::SnapshotMergeRegion>& mergeRegions)
{
    SPDLOG_DEBUG("Executor {} executing {} threads", id, req->messages_size());

    std::string funcStr = faabric::util::funcToString(req);

    // Set group ID, this will get overridden in there's a cached decision
    int groupId = faabric::util::generateGid();
    for (auto& m : *req->mutable_messages()) {
        m.set_groupid(groupId);
        m.set_groupsize(req->messages_size());
    }

    // Get the scheduling decision
    faabric::util::SchedulingDecision decision = sch.makeSchedulingDecision(
      req, faabric::util::SchedulingTopologyHint::CACHED);
    bool isSingleHost = decision.isSingleHost();

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
            SPDLOG_TRACE(
              "No updates to main thread snapshot for {} over {} pages",
              faabric::util::funcToString(msg, false),
              dirtyRegions.size());
        } else {
            SPDLOG_DEBUG("Updating main thread snapshot for {} with {} diffs",
                         faabric::util::funcToString(msg, false),
                         updates.size());
            snap->queueDiffs(updates);
            snap->writeQueuedDiffs();
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
    sch.callFunctions(req, decision);
    std::vector<std::pair<uint32_t, int32_t>> results =
      sch.awaitThreadResults(req);

    // Perform snapshot updates if not on single host
    if (!isSingleHost) {
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

void Executor::executeTasks(std::vector<int> msgIdxs,
                            std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    const std::string funcStr = faabric::util::funcToString(req);
    SPDLOG_TRACE("{} executing {}/{} tasks of {} (single-host={})",
                 id,
                 msgIdxs.size(),
                 req->messages_size(),
                 funcStr,
                 req->singlehost());

    // Note that this lock is specific to this executor, so will only block
    // when multiple threads are trying to schedule tasks. This will only
    // happen when child threads of the same function are competing to
    // schedule more threads, hence is rare so we can afford to be
    // conservative here.
    faabric::util::UniqueLock lock(threadsMutex);

    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    std::string thisHost = faabric::util::getSystemConfig().endpointHost;

    bool isMaster = firstMsg.masterhost() == thisHost;
    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;
    bool isSingleHost = req->singlehost();
    std::string snapshotKey = firstMsg.snapshotkey();

    // Threads on a single host don't need to do anything with snapshots, as
    // they all share a single executor. Threads not on a single host need to
    // restore from the main thread snapshot. Non-threads need to restore from
    // a snapshot if they are given a snapshot key.
    if (isThreads && !isSingleHost) {
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
          "Restoring threads of {} from snapshot {}", funcStr, snapKey);
        restore(snapKey);

        // Get updated memory view and start global tracking of memory
        memView = getMemoryView();
        tracker->startTracking(memView);
    } else if (!isThreads && !firstMsg.snapshotkey().empty()) {
        // Restore from snapshot if provided
        std::string snapshotKey = firstMsg.snapshotkey();
        SPDLOG_DEBUG("Restoring {} from snapshot {}", funcStr, snapshotKey);
        restore(snapshotKey);
    } else {
        SPDLOG_TRACE("Not restoring {}. threads={}, key={}, single={}",
                     funcStr,
                     isThreads,
                     snapshotKey,
                     isSingleHost);
    }

    // Set up shared counter for this batch of tasks
    auto batchCounter = std::make_shared<std::atomic<int>>(msgIdxs.size());

    // Iterate through and invoke tasks. By default, we allocate tasks
    // one-to-one with thread pool threads. Only once the pool is exhausted
    // do we start overloading
    for (int msgIdx : msgIdxs) {
        const faabric::Message& msg = req->messages().at(msgIdx);

        int threadPoolIdx = -1;
        if (availablePoolThreads.empty()) {
            // Here all threads are still executing, so we have to overload.
            // If any tasks are blocking we risk a deadlock, and can no
            // longer guarantee the application will finish. In general if
            // we're on the master host and this is a thread, we should
            // avoid the zeroth and first pool threads as they are likely to
            // be the main thread and the zeroth in the communication group,
            // so will be blocking.
            if (isThreads && isMaster) {
                if (threadPoolSize <= 2) {
                    SPDLOG_ERROR("Insufficient pool threads ({}) to "
                                 "overload {} idx {}",
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
          ExecutorTask(msgIdx, req, batchCounter));

        // Lazily create the thread
        if (threadPoolThreads.at(threadPoolIdx) == nullptr) {
            threadPoolThreads.at(threadPoolIdx) = std::make_shared<std::thread>(
              &Executor::threadPoolThread, this, threadPoolIdx);
        }
    }
}

std::shared_ptr<faabric::util::SnapshotData> Executor::getMainThreadSnapshot(
  faabric::Message& msg,
  bool createIfNotExists)
{
    std::string snapshotKey = faabric::util::getMainThreadSnapshotKey(msg);
    bool exists = false;
    {
        faabric::util::SharedLock lock(threadExecutionMutex);
        exists = reg.snapshotExists(snapshotKey);
    }

    if (!exists && createIfNotExists) {
        faabric::util::FullLock lock(threadExecutionMutex);
        if (!reg.snapshotExists(snapshotKey)) {
            SPDLOG_DEBUG("Creating main thread snapshot: {} for {}",
                         snapshotKey,
                         faabric::util::funcToString(msg, false));

            std::shared_ptr<faabric::util::SnapshotData> snap =
              std::make_shared<faabric::util::SnapshotData>(getMemoryView());
            reg.registerSnapshot(snapshotKey, snap);
        } else {
            return reg.getSnapshot(snapshotKey);
        }
    } else if (!exists) {
        SPDLOG_ERROR("No main thread snapshot {}", snapshotKey);
        throw std::runtime_error("No main thread snapshot");
    }

    return reg.getSnapshot(snapshotKey);
}

void Executor::deleteMainThreadSnapshot(const faabric::Message& msg)
{
    std::string snapshotKey = faabric::util::getMainThreadSnapshotKey(msg);

    if (reg.snapshotExists(snapshotKey)) {
        SPDLOG_DEBUG("Deleting main thread snapshot for {}",
                     faabric::util::funcToString(msg, false));

        // Broadcast the deletion
        sch.broadcastSnapshotDelete(msg, snapshotKey);

        // Delete locally
        reg.deleteSnapshot(snapshotKey);
    }
}

void Executor::threadPoolThread(int threadPoolIdx)
{
    SPDLOG_DEBUG("Thread pool thread {}:{} starting up", id, threadPoolIdx);

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

        // Start dirty tracking if executing threads across hosts
        bool isSingleHost = task.req->singlehost();
        bool isThreads =
          task.req->type() == faabric::BatchExecuteRequest::THREADS;
        bool doDirtyTracking = isThreads && !isSingleHost;
        if (doDirtyTracking) {
            // If tracking is thread local, start here as it will happen for
            // each thread
            tracker->startThreadLocalTracking(getMemoryView());
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

        // Set up context
        ExecutorContext::set(this, task.req, task.messageIndex);

        // Execute the task
        int32_t returnValue;
        bool migrated = false;
        try {
            returnValue =
              executeTask(threadPoolIdx, task.messageIndex, task.req);
        } catch (const faabric::util::FunctionMigratedException& ex) {
            SPDLOG_DEBUG(
              "Task {} migrated, shutting down executor {}", msg.id(), id);

            // Note that when a task has been migrated, we need to perform all
            // the normal executor shutdown, but we must NOT set the result for
            // the call.
            migrated = true;
            selfShutdown = true;
            returnValue = -99;

            // MPI migration
            if (msg.ismpi()) {
                auto& mpiWorld =
                  faabric::scheduler::getMpiWorldRegistry().getWorld(
                    msg.mpiworldid());
                mpiWorld.destroy();
            }
        } catch (const std::exception& ex) {
            returnValue = 1;

            std::string errorMessage = fmt::format(
              "Task {} threw exception. What: {}", msg.id(), ex.what());
            SPDLOG_ERROR(errorMessage);
            msg.set_outputdata(errorMessage);
        }

        // Unset context
        ExecutorContext::unset();

        // Handle thread-local diffing for every thread
        if (doDirtyTracking) {
            // Stop dirty tracking
            std::span<uint8_t> memView = getMemoryView();
            tracker->stopThreadLocalTracking(memView);

            // Add this thread's changes to executor-wide list of dirty regions
            auto thisThreadDirtyRegions =
              tracker->getThreadLocalDirtyPages(memView);

            faabric::util::FullLock lock(threadExecutionMutex);
            faabric::util::mergeDirtyPages(dirtyRegions,
                                           thisThreadDirtyRegions);
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

        // Handle last-in-batch dirty tracking
        if (isLastInBatch && doDirtyTracking) {
            // Stop non-thread-local tracking as we're the last in the batch
            std::span<uint8_t> memView = getMemoryView();
            tracker->stopTracking(memView);

            // Add non-thread-local dirty regions
            {
                faabric::util::FullLock lock(threadExecutionMutex);
                std::vector<char> r = tracker->getDirtyPages(memView);
                faabric::util::mergeDirtyPages(dirtyRegions, r);
            }

            // Fill snapshot gaps with overwrite regions first
            std::string mainThreadSnapKey =
              faabric::util::getMainThreadSnapshotKey(msg);
            auto snap = reg.getSnapshot(mainThreadSnapKey);
            snap->fillGapsWithBytewiseRegions();

            // Compare snapshot with all dirty regions for this executor
            std::vector<faabric::util::SnapshotDiff> diffs;
            {
                // Do the diffing
                faabric::util::FullLock lock(threadExecutionMutex);
                diffs = snap->diffWithDirtyRegions(memView, dirtyRegions);
                dirtyRegions.clear();
            }

            if (diffs.empty()) {
                SPDLOG_DEBUG("No diffs for {}", mainThreadSnapKey);
            } else {
                // On master we queue the diffs locally directly, on a remote
                // host we push them back to master
                if (isMaster) {
                    SPDLOG_DEBUG(
                      "Queueing {} diffs for {} to snapshot {} (group {})",
                      diffs.size(),
                      faabric::util::funcToString(msg, false),
                      mainThreadSnapKey,
                      msg.groupid());

                    snap->queueDiffs(diffs);
                } else if (isLastInBatch) {
                    sch.pushSnapshotDiffs(msg, mainThreadSnapKey, diffs);
                }
            }

            // If last in batch on this host, clear the merge regions
            SPDLOG_DEBUG("Clearing merge regions for {}", mainThreadSnapKey);
            snap->clearMergeRegions();
        }

        // If this is not a threads request and last in its batch, it may be
        // the main function in a threaded application, in which case we
        // want to stop any tracking and delete the main thread snapshot
        if (!isThreads && isLastInBatch) {
            // Stop tracking memory
            std::span<uint8_t> memView = getMemoryView();
            if (!memView.empty()) {
                tracker->stopTracking(memView);
                tracker->stopThreadLocalTracking(memView);

                // Delete the main thread snapshot (implicitly does nothing if
                // doesn't exist)
                deleteMainThreadSnapshot(msg);
            }
        }

        // If this batch is finished, reset the executor and release its
        // claim. Note that we have to release the claim _after_ resetting,
        // otherwise the executor won't be ready for reuse
        if (isLastInBatch) {
            // Threads skip the reset as they will be restored from their
            // respective snapshot on the next execution.
            if (isThreads) {
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

        // If the function has been migrated, we drop out here and shut down the
        // executor
        if (migrated) {
            break;
        }

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

        // We have to keep a record of dead threads so we can join them all when
        // the executor shuts down
        bool isFinished = true;
        {
            faabric::util::UniqueLock threadsLock(threadsMutex);
            std::shared_ptr<std::thread> thisThread =
              threadPoolThreads.at(threadPoolIdx);
            deadThreads.emplace_back(thisThread);

            // Make sure we're definitely not still tracking changes
            std::span<uint8_t> memView = getMemoryView();
            if (!memView.empty()) {
                tracker->stopTracking(memView);
            }

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
