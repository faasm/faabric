#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/state/State.h>
#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/queue.h>
#include <faabric/util/timing.h>

namespace faabric::scheduler {

// TODO - avoid the copy of the message here?
Executor::Executor(const faabric::Message& msg)
  : boundMessage(msg)
{
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();

    threadPoolSize = faabric::util::getUsableCores();

    // Set an ID for this Executor
    id = conf.endpointHost + "_" + std::to_string(faabric::util::generateGid());
    faabric::util::getLogger()->debug("Starting executor {}", id);
}

Executor::~Executor()
{
    finish();
}

void Executor::finish()
{
    const auto& logger = faabric::util::getLogger();
    logger->debug("Executor {} shutting down", id);

    assert(threadQueues.size() == threadPoolThreads.size());

    bool isThreads = threadPoolThreads.size() > 1;

    // Shut down thread pool with a series of kill messages
    for (auto& queuePair : threadQueues) {
        logger->trace(
          "Executor {} killing thread pool {}", id, queuePair.first);
        std::shared_ptr<BatchExecuteRequest> killReq =
          faabric::util::batchExecFactory();

        killReq->add_messages()->set_type(faabric::Message::KILL);
        queuePair.second.enqueue(std::make_pair(0, killReq));
    }

    // Wait
    logger->trace("Executor {} awaiting all non-master threads", id);
    for (auto& t : threadPoolThreads) {
        if (isThreads && t.first == 0) {
            continue;
        }

        if (t.second.joinable()) {
            t.second.join();
        }
    }

    if (threadPoolThreads.count(0) == 0) {
        logger->trace("Executor {} has no master thread", id);
    } else {
        logger->trace("Executor {} awaiting master thread", id);

        if (threadPoolThreads[0].joinable()) {
            threadPoolThreads[0].join();
        }
    }

    // Hook
    this->postFinish();

    threadQueues.clear();
    threadPoolThreads.clear();
}

void Executor::executeTasks(std::vector<int> msgIdxs,
                            std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    const auto& logger = faabric::util::getLogger();
    int nMessages = msgIdxs.size();

    const std::string funcStr = faabric::util::funcToString(req);
    logger->trace("{} executing {}/{} tasks of {}",
                  id,
                  nMessages,
                  req->messages_size(),
                  funcStr);

    // Note that this lock is specific to this executor, so will only block when
    // multiple threads are trying to schedule tasks.
    // This will only happen when child threads of the same function are
    // competing, hence is rare.
    faabric::util::UniqueLock lock(threadsMutex);

    // Restore if necessary. If we're executing threads on the master host we
    // assume we don't need to restore, but for everything else we do. If we've
    // already restored from this snapshot, we don't do so again.
    const faabric::Message& firstMsg = req->messages().at(0);
    std::string snapshotKey = firstMsg.snapshotkey();
    std::string thisHost = faabric::util::getSystemConfig().endpointHost;

    bool isMaster = firstMsg.masterhost() == thisHost;
    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;
    bool isSnapshot = !snapshotKey.empty();
    bool alreadyRestored = snapshotKey == lastSnapshot;

    if (isSnapshot && !alreadyRestored) {
        if ((!isMaster && isThreads) || !isThreads) {
            logger->debug(
              "Performing snapshot restore {} [{}]", funcStr, snapshotKey);
            lastSnapshot = snapshotKey;
            restore(firstMsg);
        } else {
            logger->debug("Skipping snapshot restore on master {} [{}]",
                          funcStr,
                          snapshotKey);
        }
    } else if (isSnapshot) {
        logger->debug(
          "Skipping already restored snapshot {} [{}]", funcStr, snapshotKey);
    }

    // Set executing task count
    executingTaskCount += msgIdxs.size();

    // Iterate through and invoke tasks
    for (int msgIdx : msgIdxs) {
        const faabric::Message& msg = req->messages().at(msgIdx);

        // If executing threads, we must always keep thread pool index zero
        // free, as this may be executing the function that spawned them
        int threadPoolIdx;
        if (isThreads) {
            threadPoolIdx = (msg.appindex() % (threadPoolSize - 1)) + 1;
        } else {
            threadPoolIdx = msg.appindex() % threadPoolSize;
        }

        // Enqueue the task
        logger->trace("Assigning task {} to {}", msgIdx, threadPoolIdx);
        threadQueues[threadPoolIdx].enqueue(std::make_pair(msgIdx, req));

        // Lazily create the thread
        if (threadPoolThreads.count(threadPoolIdx) == 0) {
            threadPoolThreads.emplace(
              std::make_pair(threadPoolIdx, [this, threadPoolIdx] {
                  auto logger = faabric::util::getLogger();
                  logger->debug(
                    "Thread pool thread {}:{} starting up", id, threadPoolIdx);

                  auto& sch = faabric::scheduler::getScheduler();
                  auto& conf = faabric::util::getSystemConfig();

                  for (;;) {
                      logger->trace(
                        "Thread starting loop {}:{}", id, threadPoolIdx);
                      std::pair<int,
                                std::shared_ptr<faabric::BatchExecuteRequest>>
                        task;

                      try {
                          task = threadQueues[threadPoolIdx].dequeue(
                            conf.boundTimeout);
                      } catch (faabric::util::QueueTimeoutException& ex) {
                          // If the thread has had no messages, it needs to
                          // remove itself
                          shutdownThreadPoolThread(threadPoolIdx);
                          break;
                      }

                      int msgIdx = task.first;
                      std::shared_ptr<faabric::BatchExecuteRequest> req =
                        task.second;
                      bool isThread =
                        req->type() == faabric::BatchExecuteRequest::THREADS;

                      faabric::Message& msg =
                        req->mutable_messages()->at(msgIdx);

                      // If the thread is being killed, the executor itself
                      // will handle the clean-up
                      if (msg.type() == faabric::Message::KILL) {
                          logger->debug("Killing thread pool thread {}:{}",
                                        id,
                                        threadPoolIdx);
                          break;
                      }

                      logger->trace("Thread {}:{} executing task {} ({})",
                                    id,
                                    threadPoolIdx,
                                    msgIdx,
                                    msg.id());

                      int32_t returnValue;
                      try {
                          returnValue = executeTask(threadPoolIdx, msgIdx, req);
                      } catch (const std::exception& ex) {
                          returnValue = 1;

                          msg.set_outputdata(
                            fmt::format("Task {} threw exception. What: {}",
                                        msg.id(),
                                        ex.what()));
                      }

                      logger->trace("Task {} finished by thread {}:{}",
                                    msg.id(),
                                    id,
                                    threadPoolIdx);
                      msg.set_returnvalue(returnValue);

                      // Decrement task count and notify if we're completely
                      // done
                      int oldTaskCount = executingTaskCount.fetch_sub(1);
                      if (oldTaskCount == 1) {
                          sch.notifyExecutorFinished(this, msg);
                      }

                      // Notify the scheduler last, as this executor may be
                      // killed instantly afterwards
                      if (isThread) {
                          sch.setThreadResult(msg, returnValue);
                      } else {
                          sch.setFunctionResult(msg);
                      }
                  }
              }));
        }
    }
}

void Executor::shutdownThreadPoolThread(int threadPoolIdx)
{
    const auto& logger = faabric::util::getLogger();
    logger->debug("Shutting down thread pool thread {}:{}", id, threadPoolIdx);

    threadPoolThreads.erase(threadPoolIdx);

    if (threadPoolThreads.empty()) {
        // Notify that we're done
        auto& sch = faabric::scheduler::getScheduler();
        sch.notifyExecutorShutdown(this, boundMessage);
    }
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

void Executor::flush() {}

void Executor::reset(const faabric::Message& msg) {}

void Executor::restore(const faabric::Message& msg) {}
}
