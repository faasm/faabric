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
                           std::shared_ptr<faabric::BatchExecuteRequest> reqIn)
  : req(std::move(reqIn))
  , messageIndex(messageIndexIn)
{}

// TODO - avoid the copy of the message here?
Executor::Executor(faabric::Message& msg)
  : boundMessage(msg)
  , sch(getScheduler())
  , reg(faabric::snapshot::getSnapshotRegistry())
  , tracker(faabric::util::getDirtyTracker())
  , threadPoolSize(faabric::util::getUsableCores())
  , lastExec(faabric::util::startTimer())
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

/**
 * Shuts down the executor and clears all its state, including its thread pool.
 *
 * This must be called before destructing an executor. This is because the
 * tidy-up requires implementations of virtual methods held in subclasses, that
 * may depend on state that those subclass instances hold. Because destructors
 * run in inheritance order, this means that state may have been destructed
 * before the executor destructor runs.
 */
void Executor::shutdown()
{
    // This method will be called during the scheduler reset and destructor,
    // hence it cannot rely on the presence of the scheduler or any of its
    // state.

    SPDLOG_DEBUG("Executor {} shutting down", id);

    for (int i = 0; i < threadPoolThreads.size(); i++) {
        // Skip any uninitialised, or already remove threads
        if (threadPoolThreads[i] == nullptr) {
            continue;
        }

        // Send a kill message
        SPDLOG_TRACE("Executor {} killing thread pool {}", id, i);
        threadTaskQueues[i].enqueue(ExecutorTask(POOL_SHUTDOWN, nullptr));

        // Wait for thread to terminate
        if (threadPoolThreads[i]->joinable()) {
            threadPoolThreads[i]->request_stop();
            threadPoolThreads[i]->join();
        }

        // Mark as killed
        threadPoolThreads[i] = nullptr;
    }

    _isShutdown = true;
}

Executor::~Executor()
{
    if (!_isShutdown) {
        SPDLOG_ERROR("Destructing Executor {} without shutting down first", id);
    }
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

    // Deregister the threads
    sch.deregisterThreads(req);

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

    // Update the last-executed time for this executor
    lastExec = faabric::util::startTimer();

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

        // Prepare list of lists for dirty pages from each thread
        threadLocalDirtyRegions.resize(req->messages_size());
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

    // Initialise batch counter
    batchCounter.store(msgIdxs.size());

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
        threadTaskQueues[threadPoolIdx].enqueue(ExecutorTask(msgIdx, req));

        // Lazily create the thread
        if (threadPoolThreads.at(threadPoolIdx) == nullptr) {
            threadPoolThreads.at(threadPoolIdx) =
              std::make_shared<std::jthread>(
                std::bind_front(&Executor::threadPoolThread, this),
                threadPoolIdx);
        }
    }
}

long Executor::getMillisSinceLastExec()
{
    return faabric::util::getTimeDiffMillis(lastExec);
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
              std::make_shared<faabric::util::SnapshotData>(getMemoryView(),
                                                            getMaxMemorySize());
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

void Executor::threadPoolThread(std::stop_token st, int threadPoolIdx)
{
    SPDLOG_DEBUG("Thread pool thread {}:{} starting up", id, threadPoolIdx);

    const auto& conf = faabric::util::getSystemConfig();

    // We terminate these threads by sending a shutdown message, but having this
    // check means they won't hang infinitely if destructed.
    while (!st.stop_requested()) {
        SPDLOG_TRACE("Thread starting loop {}:{}", id, threadPoolIdx);

        ExecutorTask task;

        try {
            task = threadTaskQueues[threadPoolIdx].dequeue(conf.boundTimeout);
        } catch (faabric::util::QueueTimeoutException& ex) {
            SPDLOG_TRACE(
              "Thread {}:{} got no messages in timeout {}ms, looping",
              id,
              threadPoolIdx,
              conf.boundTimeout);

            continue;
        }

        // If the thread is being killed, the executor itself
        // will handle the clean-up
        if (task.messageIndex == POOL_SHUTDOWN) {
            SPDLOG_DEBUG("Killing thread pool thread {}:{}", id, threadPoolIdx);
            return;
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
        try {
            returnValue =
              executeTask(threadPoolIdx, task.messageIndex, task.req);
        } catch (const faabric::util::FunctionMigratedException& ex) {
            SPDLOG_DEBUG(
              "Task {} migrated, shutting down executor {}", msg.id(), id);

            // Note that when a task has been migrated, we need to perform all
            // the normal executor shutdown, and we set a special return value
            returnValue = MIGRATED_FUNCTION_RETURN_VALUE;

            // MPI migration
            if (msg.ismpi()) {
                auto& mpiWorld =
                  faabric::scheduler::getMpiWorldRegistry().getWorld(
                    msg.mpiworldid());
                mpiWorld.destroy();
            }
        } catch (const std::exception& ex) {
            returnValue = 1;

            std::string errorMessage =
              fmt::format("Task {}:{}:{} threw exception. What: {}",
                          msg.appid(),
                          msg.groupid(),
                          msg.groupidx(),
                          ex.what());
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

            // Record this thread's dirty regions
            threadLocalDirtyRegions[task.messageIndex] = thisThreadDirtyRegions;
        }

        // Set the return value
        msg.set_returnvalue(returnValue);

        // Decrement the task count. If the counter reaches zero at this point,
        // all other tasks must also have reached this point, hence we can
        // guarantee that the other tasks have also finished, and perform any
        // merging or tidying up.
        std::atomic_thread_fence(std::memory_order_release);
        int oldTaskCount = batchCounter.fetch_sub(1);
        assert(oldTaskCount >= 0);
        bool isLastInBatch = oldTaskCount == 1;

        SPDLOG_TRACE("Task {} finished by thread {}:{} ({} left)",
                     faabric::util::funcToString(msg, true),
                     id,
                     threadPoolIdx,
                     oldTaskCount - 1);

        // Handle last-in-batch dirty tracking
        std::string mainThreadSnapKey =
          faabric::util::getMainThreadSnapshotKey(msg);
        std::vector<faabric::util::SnapshotDiff> diffs;
        if (isLastInBatch && doDirtyTracking) {
            // Stop non-thread-local tracking as we're the last in the batch
            std::span<uint8_t> memView = getMemoryView();
            tracker->stopTracking(memView);

            // Merge all dirty regions
            {
                faabric::util::FullLock lock(threadExecutionMutex);

                // Merge together regions from all threads
                faabric::util::mergeManyDirtyPages(dirtyRegions,
                                                   threadLocalDirtyRegions);

                // Clear thread-local dirty regions, no longer needed
                threadLocalDirtyRegions.clear();

                // Merge the globally tracked regions
                std::vector<char> globalDirtyRegions =
                  tracker->getDirtyPages(memView);
                faabric::util::mergeDirtyPages(dirtyRegions,
                                               globalDirtyRegions);
            }

            // Fill snapshot gaps with overwrite regions first
            auto snap = reg.getSnapshot(mainThreadSnapKey);
            snap->fillGapsWithBytewiseRegions();

            // Compare snapshot with all dirty regions for this executor
            {
                // Do the diffing
                faabric::util::FullLock lock(threadExecutionMutex);
                diffs = snap->diffWithDirtyRegions(memView, dirtyRegions);
                dirtyRegions.clear();
            }

            // If last in batch on this host, clear the merge regions (only
            // needed for doing the diffing on the current host)
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

        // Finally set the result of the task, this will allow anything
        // waiting on its result to continue execution, therefore must be
        // done once the executor has been reset, otherwise the executor may
        // not be reused for a repeat invocation.
        if (isThreads) {
            // Set non-final thread result
            if (isLastInBatch) {
                // Include diffs if this is the last one
                sch.setThreadResult(msg, returnValue, mainThreadSnapKey, diffs);
            } else {
                sch.setThreadResult(msg, returnValue, "", {});
            }
        } else {
            // Set normal function result
            sch.setFunctionResult(msg);
        }
    }
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

size_t Executor::getMaxMemorySize()
{
    SPDLOG_WARN("Executor has not implemented max memory size method");

    return 0;
}

faabric::Message& Executor::getBoundMessage()
{
    return boundMessage;
}

bool Executor::isExecuting()
{
    int currentCount = batchCounter.load();
    return currentCount > 0;
}

void Executor::restore(const std::string& snapshotKey)
{
    std::span<uint8_t> memView = getMemoryView();
    if (memView.empty()) {
        SPDLOG_ERROR("No memory on {} to restore {}", id, snapshotKey);
        throw std::runtime_error("No memory to restore executor");
    }

    // Expand memory if necessary
    auto snap = reg.getSnapshot(snapshotKey);
    setMemorySize(snap->getSize());

    // Map the memory onto the snapshot
    snap->mapToMemory({ memView.data(), snap->getSize() });
}
}
