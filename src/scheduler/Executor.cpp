#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/queue.h>
#include <faabric/util/timing.h>

#define POOL_SHUTDOWN -1

namespace faabric::scheduler {

ExecutorTask::ExecutorTask(int messageIndexIn,
                           std::shared_ptr<faabric::BatchExecuteRequest> reqIn,
                           std::shared_ptr<std::atomic<int>> batchCounterIn,
                           bool needsSnapshotPushIn,
                           bool skipResetIn)
  : messageIndex(messageIndexIn)
  , req(reqIn)
  , batchCounter(batchCounterIn)
  , needsSnapshotPush(needsSnapshotPushIn)
  , skipReset(skipResetIn)
{}

// TODO - avoid the copy of the message here?
Executor::Executor(faabric::Message& msg)
  : boundMessage(msg)
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

Executor::~Executor() {}

void Executor::finish()
{
    SPDLOG_DEBUG("Executor {} shutting down", id);

    // Shut down thread pools and wait
    for (int i = 0; i < threadPoolThreads.size(); i++) {
        if (threadPoolThreads.at(i) == nullptr) {
            continue;
        }

        // Send a kill message
        SPDLOG_TRACE("Executor {} killing thread pool {}", id, i);
        threadTaskQueues[i].enqueue(
          ExecutorTask(POOL_SHUTDOWN, nullptr, nullptr, false, false));

        // Await the thread
        if (threadPoolThreads.at(i)->joinable()) {
            threadPoolThreads.at(i)->join();
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

    lastSnapshot = "";

    claimed = false;

    threadPoolThreads.clear();
    threadTaskQueues.clear();
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

    // Restore if necessary. If we're executing threads on the master host we
    // assume we don't need to restore, but for everything else we do. If we've
    // already restored from this snapshot, we don't do so again.
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    std::string snapshotKey = firstMsg.snapshotkey();
    std::string thisHost = faabric::util::getSystemConfig().endpointHost;

    bool isMaster = firstMsg.masterhost() == thisHost;
    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;
    bool isSnapshot = !snapshotKey.empty();
    bool alreadyRestored = snapshotKey == lastSnapshot;

    if (isSnapshot && !alreadyRestored) {
        if ((!isMaster && isThreads) || !isThreads) {
            SPDLOG_DEBUG("Restoring {} from snapshot {}", funcStr, snapshotKey);
            lastSnapshot = snapshotKey;
            restore(firstMsg);
        } else {
            SPDLOG_DEBUG("Skipping snapshot restore on master {} [{}]",
                         funcStr,
                         snapshotKey);
        }
    } else if (isSnapshot) {
        SPDLOG_DEBUG(
          "Skipping already restored snapshot {} [{}]", funcStr, snapshotKey);
    }

    // Reset dirty page tracking if we're executing threads.
    // Note this must be done after the restore has happened
    bool needsSnapshotPush = false;
    if (isThreads && isSnapshot && !isMaster) {
        faabric::util::resetDirtyTracking();
        needsSnapshotPush = true;
    }

    // Set up shared counter for this batch of tasks
    auto batchCounter =
      std::make_shared<std::atomic<int>>(req->messages_size());

    // Work out if we should skip the reset after this batch. This only needs to
    // happen when we're executing threads on the master host, in which case the
    // original function call will cause a reset
    bool skipReset = isMaster && isThreads;

    // Iterate through and invoke tasks. By default, we allocate tasks
    // one-to-one with thread pool threads. Only once the pool is exhausted do
    // we start overallocating.
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
                assert(threadPoolSize > 2);
                threadPoolIdx = (msg.appidx() % (threadPoolSize - 2)) + 2;
            } else {
                threadPoolIdx = msg.appidx() % threadPoolSize;
            }

            SPDLOG_WARN("Overloaded app index {} to thread {}",
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
          msgIdx, req, batchCounter, needsSnapshotPush, skipReset));

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

        bool isThreads =
          task.req->type() == faabric::BatchExecuteRequest::THREADS;
        SPDLOG_TRACE("Thread {}:{} executing task {} ({}, thread={})",
                     id,
                     threadPoolIdx,
                     task.messageIndex,
                     msg.id(),
                     isThreads);

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

        // Set the return value
        msg.set_returnvalue(returnValue);

        // Decrement the task count
        int oldTaskCount = task.batchCounter->fetch_sub(1);
        assert(oldTaskCount >= 0);
        bool isLastInBatch = oldTaskCount == 1;

        SPDLOG_TRACE("Task {} finished by thread {}:{} ({} left)",
                     faabric::util::funcToString(msg, true),
                     id,
                     threadPoolIdx,
                     oldTaskCount - 1);

        // Handle snapshot diffs _before_ we reset the executor
        if (isLastInBatch && task.needsSnapshotPush) {
            // Get diffs between original snapshot and after execution
            faabric::util::SnapshotData snapshotPostExecution = snapshot();

            faabric::util::SnapshotData snapshotPreExecution =
              faabric::snapshot::getSnapshotRegistry().getSnapshot(
                msg.snapshotkey());

            std::vector<faabric::util::SnapshotDiff> diffs =
              snapshotPreExecution.getChangeDiffs(snapshotPostExecution.data,
                                                  snapshotPostExecution.size);

            sch.pushSnapshotDiffs(msg, diffs);

            // Reset dirty page tracking now that we've pushed the diffs
            faabric::util::resetDirtyTracking();
        }

        // In all threads if we've finished with a ptp group, we need to tidy up
        // to ensure all sockets can be destructed
        if (msg.groupid() > 0) {
            broker.resetThreadLocalCache(true);

            // Remove the group from this host if last in batch
            if (isLastInBatch) {
                broker.clearGroup(msg.groupid());
            }
        }

        // If this batch is finished, reset the executor and release its claim.
        // Note that we have to release the claim _after_ resetting, otherwise
        // the executor won't be ready for reuse.
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
        // releasing the claim on this executor, otherwise the scheduler may try
        // to schedule another function and be unable to reuse this executor.
        sch.vacateSlot();

        // Finally set the result of the task, this will allow anything waiting
        // on its result to continue execution, therefore must be done once the
        // executor has been reset, otherwise the executor may not be reused for
        // a repeat invocation.
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

        // Note - we have to keep a record of dead threads so we can join them
        // all when the executor shuts down
        std::shared_ptr<std::thread> thisThread =
          threadPoolThreads.at(threadPoolIdx);
        deadThreads.emplace_back(thisThread);

        // Set this thread to nullptr
        threadPoolThreads.at(threadPoolIdx) = nullptr;

        // See if any threads are still running
        bool isFinished = true;
        for (auto t : threadPoolThreads) {
            if (t != nullptr) {
                isFinished = false;
                break;
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

faabric::util::SnapshotData Executor::snapshot()
{
    SPDLOG_WARN("Executor has not implemented snapshot method");
    faabric::util::SnapshotData d;
    return d;
}

void Executor::restore(faabric::Message& msg)
{
    SPDLOG_WARN("Executor has not implemented restore method");
}
}
