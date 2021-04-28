#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/state/State.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/queue.h>

namespace faabric::scheduler {
Executor::Executor(Scheduler& sch, const faabric::Message& msg)
  : scheduler(sch)
  , boundMessage(msg)
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();

    // Note that the main thread will be executing so we want one less
    threadPoolSize = faabric::util::getUsableCores() - 1;

    // Set an ID for this Faaslet
    id = conf.endpointHost + "_" + std::to_string(faabric::util::generateGid());

    // Hook
    this->postBind(msg);
}

void Executor::finish()
{
    // Notify scheduler if this thread was bound to a function
    scheduler.notifyFaasletFinished(boundMessage);

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
    scheduler.notifyCallFinished(msg);

    // Set result
    logger->debug("Setting function result for {}", funcStr);
    scheduler.setFunctionResult(msg);

    // Increment the execution counter
    executionCount++;

    // Hook
    this->postFinishCall();
}

void Executor::batchExecuteThreads(faabric::scheduler::MessageTask& task)
{
    const auto& logger = faabric::util::getLogger();
    std::vector<int> messageIdxs = task.first;
    int nMessages = messageIdxs.size();
    std::shared_ptr<faabric::BatchExecuteRequest> req = task.second;

    const std::string funcStr = faabric::util::funcToString(req);
    logger->info("Batch executing {}/{} threads of {}",
                 nMessages,
                 req->messages_size(),
                 funcStr);

    // Iterate through and invoke threads
    for (int msgIdx : messageIdxs) {
        const faabric::Message& msg = req->messages().at(msgIdx);
        int threadPoolIdx = msg.appindex() % threadPoolSize;

        std::pair<int, std::shared_ptr<faabric::BatchExecuteRequest>> task =
          std::make_pair(msgIdx, req);
        threadQueues[threadPoolIdx].enqueue(std::move(task));

        if (threads.count(threadPoolIdx) == 0) {
            // Get mutex and re-check
            faabric::util::UniqueLock lock(threadsMutex);

            if (threads.count(threadPoolIdx) == 0) {
                threads.emplace(
                  std::make_pair(threadPoolIdx, [this, threadPoolIdx] {
                      auto logger = faabric::util::getLogger();
                      logger->debug("Thread pool thread {} starting up",
                                    threadPoolIdx);

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

                          int32_t returnValue =
                            executeThread(threadPoolIdx, req, msg);

                          // Set the result for this thread
                          auto& sch = faabric::scheduler::getScheduler();
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
}

std::string Executor::executeCall(faabric::Message& call)
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();
    const std::string funcStr = faabric::util::funcToString(call, true);

    // Create and execute the module
    bool success;
    std::string errorMessage;
    try {
        success = this->doExecute(call);
    } catch (const std::exception& e) {
        errorMessage = "Error: " + std::string(e.what());
        logger->error(errorMessage);
        success = false;
        call.set_returnvalue(1);
    }

    if (!success && errorMessage.empty()) {
        errorMessage =
          "Call failed (return value=" + std::to_string(call.returnvalue()) +
          ")";
    }

    this->finishCall(call, success, errorMessage);
    return errorMessage;
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

void Executor::postBind(const faabric::Message& msg) {}

void Executor::preFinishCall(faabric::Message& call,
                             bool success,
                             const std::string& errorMsg)
{}

void Executor::postFinishCall() {}

void Executor::postFinish() {}

void Executor::flush() {}
}
