#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/state/State.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/queue.h>

namespace faabric::scheduler {
Executor::Executor(const faabric::Message& msg)
  : boundMessage(msg)
{
    const auto& logger = faabric::util::getLogger();

    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();

    threadPoolSize = faabric::util::getUsableCores();

    // Set an ID for this Faaslet
    id = conf.endpointHost + "_" + std::to_string(faabric::util::generateGid());
}

Executor::~Executor() {}

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
    for (auto& t : threads) {
        if (t.second.joinable()) {
            t.second.join();
        }
    }

    // Hook
    this->postFinish();
}

void Executor::finishCall(faabric::Message& msg,
                          bool success,
                          const std::string& errorMsg)
{
    // Hook
    this->preFinishCall(msg, success, errorMsg);

    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

    const std::string funcStr = faabric::util::funcToString(msg, true);
    logger->info("Finished {}", funcStr);
    if (!success) {
        msg.set_outputdata(errorMsg);
    }

    fflush(stdout);

    // Notify the scheduler *before* setting the result. Calls awaiting
    // the result will carry on blocking
    auto& sch = faabric::scheduler::getScheduler();
    sch.notifyCallFinished(msg);

    // Set result
    logger->debug("Setting function result for {}", funcStr);
    sch.setFunctionResult(msg);

    // Hook
    this->postFinishCall();
}

void Executor::executeTask(int threadPoolIdx,
                           int msgIdx,
                           std::shared_ptr<faabric::BatchExecuteRequest> req,
                           bool isThread)
{

    std::pair<int, std::shared_ptr<faabric::BatchExecuteRequest>> task =
      std::make_pair(msgIdx, req);
    threadQueues[threadPoolIdx].enqueue(std::move(task));

    if (threads.count(threadPoolIdx) == 0) {
        // Get mutex and re-check
        faabric::util::UniqueLock lock(threadsMutex);

        if (threads.count(threadPoolIdx) == 0) {
            threads.emplace(
              std::make_pair(threadPoolIdx, [this, threadPoolIdx, isThread] {
                  auto logger = faabric::util::getLogger();
                  logger->debug("Thread pool thread {} starting up",
                                threadPoolIdx);

                  auto& sch = faabric::scheduler::getScheduler();

                  for (;;) {
                      auto task = threadQueues[threadPoolIdx].dequeue();

                      int msgIdx = task.first;
                      std::shared_ptr<faabric::BatchExecuteRequest> req =
                        task.second;

                      faabric::Message& msg =
                        req->mutable_messages()->at(msgIdx);

                      if (msg.type() == faabric::Message::KILL) {
                          break;
                      }

                      int32_t returnValue;
                      if (isThread) {
                          returnValue = executeThread(threadPoolIdx, req, msg);
                      } else {
                          returnValue = doExecute(msg);
                      }

                      // Set the result for this thread
                      sch.setThreadResult(msg, returnValue);

                      // Notify scheduler finished
                      sch.notifyCallFinished(msg);
                  }

                  logger->debug("Thread pool thread {} shutting down",
                                threadPoolIdx);
              }));
        }
    }
}

void Executor::batchExecuteThreads(
  std::vector<int> msgIdxs,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    const auto& logger = faabric::util::getLogger();
    int nMessages = msgIdxs.size();

    const std::string funcStr = faabric::util::funcToString(req);
    logger->info("Batch executing {}/{} threads of {}",
                 nMessages,
                 req->messages_size(),
                 funcStr);

    // Iterate through and invoke threads
    for (int msgIdx : msgIdxs) {
        const faabric::Message& msg = req->messages().at(msgIdx);
        int threadPoolIdx = msg.appindex() % threadPoolSize;

        executeTask(threadPoolIdx, msgIdx, req, true);
    }
}

std::string Executor::executeFunction(
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();
    const std::string funcStr = faabric::util::funcToString(req);

    executeTask(0, msgIdx, req, false);

    faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    std::string resultStr = "Success";
    finishCall(msg, true, resultStr);

    return resultStr;
}

// ------------------------------------------
// HOOKS
// ------------------------------------------

bool Executor::doExecute(faabric::Message& msg)
{
    return true;
}

int32_t Executor::executeThread(
  int threadPoolIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::Message& msg)
{
    return 0;
}

void Executor::preFinishCall(faabric::Message& call,
                             bool success,
                             const std::string& errorMsg)
{}

void Executor::postFinishCall() {}

void Executor::flush() {}
}
