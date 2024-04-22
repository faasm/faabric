#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/executor/Executor.h>
#include <faabric/executor/ExecutorContext.h>
#include <faabric/executor/ExecutorTask.h>
#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/dirty.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/queue.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/string_tools.h>
#include <faabric/util/timing.h>

// Default snapshot size here is set to support 32-bit WebAssembly, but could be
// made configurable on the function call or language.
#define ONE_MB (1024L * 1024L)
#define ONE_GB (1024L * ONE_MB)
#define DEFAULT_MAX_SNAP_SIZE (4 * ONE_GB)

#define POOL_SHUTDOWN -1

namespace faabric::executor {

// TODO - avoid the copy of the message here?
Executor::Executor(faabric::Message& msg)
  : boundMessage(msg)
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

// TODO(thread-opt): get rid of this method here and move to
// PlannerClient::callFunctions()
void Executor::executeTasks(std::vector<int> msgIdxs,
                            std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    const std::string funcStr = faabric::util::funcToString(req);
    SPDLOG_TRACE("{} executing {}/{} tasks of {}",
                 id,
                 msgIdxs.size(),
                 req->messages_size(),
                 funcStr);

    // Note that this lock is specific to this executor, so will only block
    // when multiple threads are trying to schedule tasks. This will only
    // happen when child threads of the same function are competing to
    // schedule more threads, hence is rare so we can afford to be
    // conservative here.
    faabric::util::UniqueLock lock(threadsMutex);

    // Update the last-executed time for this executor
    lastExec = faabric::util::startTimer();

    auto& firstMsg = req->mutable_messages()->at(0);
    std::string thisHost = faabric::util::getSystemConfig().endpointHost;

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
        SPDLOG_TRACE("Not restoring {}. threads={}, key={}",
                     funcStr,
                     isThreads,
                     snapshotKey);
    }

    // Initialise batch counter
    if (isThreads) {
        threadBatchCounter.fetch_add(msgIdxs.size(), std::memory_order_release);
    } else {
        batchCounter.fetch_add(msgIdxs.size(), std::memory_order_release);
    }

    // Iterate through and invoke tasks. By default, we allocate tasks
    // one-to-one with thread pool threads. Only once the pool is exhausted
    // do we start overloading
    for (int msgIdx : msgIdxs) {
        [[maybe_unused]] const faabric::Message& msg =
          req->messages().at(msgIdx);

        if (availablePoolThreads.empty()) {
            SPDLOG_ERROR("No available thread pool threads (size: {})",
                         threadPoolSize);
            throw std::runtime_error("No available thread pool threads!");
        }

        // Take next from those that are available
        int threadPoolIdx = *availablePoolThreads.begin();
        availablePoolThreads.erase(threadPoolIdx);

        SPDLOG_TRACE(
          "Assigned app index {} to thread {}", msg.appidx(), threadPoolIdx);

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

/* TODO(thread-opt): currently we never delete snapshots
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
*/

void Executor::setThreadResult(
  faabric::Message& msg,
  int32_t returnValue,
  const std::string& key,
  const std::vector<faabric::util::SnapshotDiff>& diffs)
{
    bool isMaster =
      msg.mainhost() == faabric::util::getSystemConfig().endpointHost;
    if (isMaster) {
        if (!diffs.empty()) {
            // On main we queue the diffs locally directly, on a remote
            // host we push them back to main
            SPDLOG_DEBUG("Queueing {} diffs for {} to snapshot {} (group {})",
                         diffs.size(),
                         faabric::util::funcToString(msg, false),
                         key,
                         msg.groupid());

            auto snap = reg.getSnapshot(key);

            // Here we don't have ownership over all of the snapshot diff data,
            // but that's ok as the executor memory will outlast the snapshot
            // merging operation.
            snap->queueDiffs(diffs);
        }
    } else {
        // Push thread result and diffs together
        faabric::snapshot::getSnapshotClient(msg.mainhost())
          ->pushThreadResult(msg.appid(), msg.id(), returnValue, key, diffs);
    }

    // Finally, set the message result in the planner
    faabric::planner::getPlannerClient().setMessageResult(
      std::make_shared<faabric::Message>(msg));
}

void Executor::threadPoolThread(std::stop_token st, int threadPoolIdx)
{
    SPDLOG_DEBUG("Thread pool thread {}:{} starting up", id, threadPoolIdx);

    const auto conf = faabric::util::getSystemConfig();

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
        bool isThreads =
          task.req->type() == faabric::BatchExecuteRequest::THREADS;
        bool doDirtyTracking = isThreads && !task.req->singlehost();
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

        // If the to-be-executed message is a migrated message, we need to
        // execute the post-migration hook to sync with non-migrated messages
        // in the same group
        bool isMigration =
          task.req->type() == faabric::BatchExecuteRequest::MIGRATION;

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
            // Right before executing the task, do any kind of synchronisation
            // if the task has been migrated. Also benefit from the try/catch
            // statement in case the migration fails
            if (isMigration) {
                faabric::transport::getPointToPointBroker().postMigrationHook(
                  msg);
            }

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
                auto& mpiWorldRegistry = faabric::mpi::getMpiWorldRegistry();
                if (mpiWorldRegistry.worldExists(msg.mpiworldid())) {
                    bool mustClear =
                      mpiWorldRegistry.getWorld(msg.mpiworldid()).destroy();

                    if (mustClear) {
                        SPDLOG_DEBUG("{}:{}:{} clearing world {} from host {}",
                                     msg.appid(),
                                     msg.groupid(),
                                     msg.groupidx(),
                                     msg.mpiworldid(),
                                     msg.executedhost());

                        mpiWorldRegistry.clearWorld(msg.mpiworldid());
                    }
                }
            }
        } catch (const std::exception& ex) {
            returnValue = 1;

            std::string errorMessage = fmt::format(
              "Task {} threw exception. What: {}", msg.id(), ex.what());
            SPDLOG_ERROR(errorMessage);
            msg.set_outputdata(errorMessage);

            // MPI-specific clean-up after we throw an exception
            if (msg.ismpi()) {
                auto& mpiWorldRegistry = faabric::mpi::getMpiWorldRegistry();
                if (mpiWorldRegistry.worldExists(msg.mpiworldid())) {
                    mpiWorldRegistry.getWorld(msg.mpiworldid()).destroy();
                }
            }
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

        // Decrement the task count. If we are the last thread in a batch of
        // we can either be in the main host, in which case there's still the
        // main thread in the same Executor, or in a remote host, in which case
        // we need to only release the Executor after we are done. If we
        // are the main thread, we will always reset and release the executor
        std::atomic_thread_fence(std::memory_order_release);
        int oldTaskCount = 0;
        bool isLastThreadInBatch = false;
        bool isLastThreadInExecutor = false;
        if (isThreads) {
            oldTaskCount = threadBatchCounter.fetch_sub(1);
            isLastThreadInBatch = oldTaskCount == 1;
            isLastThreadInExecutor =
              batchCounter.load(std::memory_order_release) == 0;
        } else {
            oldTaskCount = batchCounter.fetch_sub(1);
            isLastThreadInExecutor = oldTaskCount == 1;
        }
        assert(oldTaskCount >= 1);

        SPDLOG_TRACE("Task {} finished by thread {}:{} ({} left)",
                     faabric::util::funcToString(msg, true),
                     id,
                     threadPoolIdx,
                     oldTaskCount - 1);

        // Handle last-in-batch dirty tracking if we are last thread in a
        // not-single-host execution, and are not on the main host (on the
        // main host we still have the zero-th thread executing)
        auto mainThreadSnapKey = faabric::util::getMainThreadSnapshotKey(msg);
        std::vector<faabric::util::SnapshotDiff> diffs;
        // FIXME: thread 0 locally is not part of this batch, but is still
        // in the same executor
        bool isRemoteThread =
          task.req->messages(0).mainhost() != conf.endpointHost;
        if (isLastThreadInBatch && doDirtyTracking && isRemoteThread) {
            diffs = mergeDirtyRegions(msg);
        }

        // If this is not a threads request and last in its batch, it may be
        // the main function (thread) in a threaded application, in which case
        // we want to stop any tracking and delete the main thread snapshot
        /* FIXME: remove me
        if (!isThreads && isLastThreadInExecutor) {
            // Stop tracking memory
            std::span<uint8_t> memView = getMemoryView();
            if (!memView.empty()) {
                tracker->stopTracking(memView);
                tracker->stopThreadLocalTracking(memView);

                // Delete the main thread snapshot (implicitly does nothing if
                // doesn't exist)
                // TODO(thread-opt): cleanup snapshots (from planner maybe?)
                // deleteMainThreadSnapshot(msg);
            }
        }
        */

        // If this batch is finished, reset the executor and release its
        // claim. Note that we have to release the claim _after_ resetting,
        // otherwise the executor won't be ready for reuse
        if (isLastThreadInExecutor) {
            // Threads skip the reset as they will be restored from their
            // respective snapshot on the next execution.
            if (isThreads) {
                SPDLOG_TRACE("Skipping reset for {} ({})",
                             faabric::util::funcToString(msg, true),
                             msg.appidx());
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

        // Finally set the result of the task, this will allow anything
        // waiting on its result to continue execution, therefore must be
        // done once the executor has been reset, otherwise the executor may
        // not be reused for a repeat invocation.
        if (isThreads) {
            // Set non-final thread result
            if (isLastThreadInBatch) {
                // Include diffs if this is the last one
                setThreadResult(msg, returnValue, mainThreadSnapKey, diffs);
            } else {
                setThreadResult(msg, returnValue, "", {});
            }
        } else {
            // Set normal function result
            faabric::planner::getPlannerClient().setMessageResult(
              std::make_shared<faabric::Message>(msg));
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

void Executor::reset(faabric::Message& msg)
{
    faabric::util::UniqueLock lock(threadsMutex);

    chainedMessages.clear();
}

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
    int currentCount = batchCounter.load(std::memory_order_acquire);
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

void Executor::addChainedMessage(const faabric::Message& msg)
{
    faabric::util::UniqueLock lock(threadsMutex);

    auto it = chainedMessages.find(msg.id());
    if (it != chainedMessages.end()) {
        SPDLOG_ERROR("Message {} already in chained messages!", msg.id());
        throw ChainedCallException("Message already registered!");
    }

    chainedMessages[msg.id()] = std::make_shared<faabric::Message>(msg);
}

const faabric::Message& Executor::getChainedMessage(int messageId)
{
    faabric::util::UniqueLock lock(threadsMutex);

    auto it = chainedMessages.find(messageId);
    if (it == chainedMessages.end()) {
        SPDLOG_ERROR("Message {} does not correspond to a chained function!",
                     messageId);
        throw ChainedCallException(
          "Unrecognised message ID for chained function");
    }

    return *(it->second);
}

std::vector<faabric::util::SnapshotDiff> Executor::mergeDirtyRegions(
  const Message& msg,
  const std::vector<char>& extraDirtyPages)
{
    std::vector<faabric::util::SnapshotDiff> diffs;
    auto mainThreadSnapKey = faabric::util::getMainThreadSnapshotKey(msg);

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
        std::vector<char> globalDirtyRegions = tracker->getDirtyPages(memView);
        faabric::util::mergeDirtyPages(dirtyRegions, globalDirtyRegions);
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

    // FIXME: is it very expensive to return these diffs?
    return diffs;
}

std::set<unsigned int> Executor::getChainedMessageIds()
{
    faabric::util::UniqueLock lock(threadsMutex);

    std::set<unsigned int> returnSet;
    for (auto it : chainedMessages) {
        returnSet.insert(it.first);
    }

    return returnSet;
}
}
