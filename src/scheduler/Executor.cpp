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
    // Shut down thread pool with a series of kill messages
    for (auto& queuePair : threadQueues) {
        std::shared_ptr<BatchExecuteRequest> killReq =
          faabric::util::batchExecFactory();

        killReq->add_messages()->set_type(faabric::Message::KILL);
        queuePair.second.enqueue(std::make_pair(0, killReq));
    }

    // Wait
    for (auto& t : threadPoolThreads) {
        if (t.second.joinable()) {
            t.second.join();
        }
    }

    threadQueues.clear();

    // Hook
    this->postFinish();
}

void Executor::executeTasks(std::vector<int> msgIdxs,
                            std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    const auto& logger = faabric::util::getLogger();
    int nMessages = msgIdxs.size();

    const std::string funcStr = faabric::util::funcToString(req);
    logger->info(
      "Executing {}/{} tasks of {}", nMessages, req->messages_size(), funcStr);

    // Restore if necessary
    const faabric::Message& firstMsg = req->messages().at(0);
    std::string snapshotKey = firstMsg.snapshotkey();
    if (!snapshotKey.empty() && (snapshotKey != lastSnapshot)) {
        faabric::util::UniqueLock lock(threadsMutex);

        if (snapshotKey != lastSnapshot) {
            lastSnapshot = snapshotKey;
            restore(firstMsg);
        }
    }

    // Set executing task count
    executingTaskCount += msgIdxs.size();

    // Iterate through and invoke tasks
    for (int msgIdx : msgIdxs) {
        const faabric::Message& msg = req->messages().at(msgIdx);
        int threadPoolIdx = msg.appindex() % threadPoolSize;

        // Enqueue the task
        std::pair<int, std::shared_ptr<faabric::BatchExecuteRequest>> task =
          std::make_pair(msgIdx, req);
        threadQueues[threadPoolIdx].enqueue(std::move(task));

        // Lazily create the thread
        if (threadPoolThreads.count(threadPoolIdx) == 0) {
            // Get mutex and re-check
            faabric::util::UniqueLock lock(threadsMutex);

            if (threadPoolThreads.count(threadPoolIdx) == 0) {

                threadPoolThreads.emplace(
                  std::make_pair(threadPoolIdx, [this, threadPoolIdx] {
                      auto logger = faabric::util::getLogger();
                      logger->debug("Thread pool thread {} starting up",
                                    threadPoolIdx);

                      auto& sch = faabric::scheduler::getScheduler();
                      auto& conf = faabric::util::getSystemConfig();

                      for (;;) {
                          std::pair<
                            int,
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
                          bool isThread = req->type() ==
                                          faabric::BatchExecuteRequest::THREADS;

                          faabric::Message& msg =
                            req->mutable_messages()->at(msgIdx);

                          // If the thread is being killed, the executor itself
                          // will handle the clean-up
                          if (msg.type() == faabric::Message::KILL) {
                              break;
                          }

                          int32_t returnValue =
                            executeTask(threadPoolIdx, msgIdx, req);
                          msg.set_returnvalue(returnValue);

                          // Notify finished
                          if (isThread) {
                              sch.setThreadResult(msg, returnValue);
                          } else {
                              sch.setFunctionResult(msg);
                          }

                          // Decrement task count and notify if we're completely
                          // done
                          int oldTaskCount = executingTaskCount.fetch_sub(1);
                          if (oldTaskCount == 1) {
                              sch.notifyExecutorFinished(this, msg);
                          }
                      }
                  }));
            }
        }
    }
}

void Executor::shutdownThreadPoolThread(int threadPoolIdx)
{
    const auto& logger = faabric::util::getLogger();
    logger->debug("Thread pool thread {} shutting down", threadPoolIdx);

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

void Executor::restore(const faabric::Message& msg) {}
}
