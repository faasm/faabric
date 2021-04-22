#include "faabric/util/queue.h"
#include <faabric/executor/FaabricExecutor.h>

#include <faabric/state/State.h>
#include <faabric/util/config.h>

namespace faabric::executor {
FaabricExecutor::FaabricExecutor(int threadIdxIn)
  : threadIdx(threadIdxIn)
  , scheduler(faabric::scheduler::getScheduler())
  , threadPoolSize(faabric::util::getSystemConfig().executorThreadPoolSize)
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

    // Set an ID for this Faaslet
    id = faabric::util::getSystemConfig().endpointHost + "_" +
         std::to_string(threadIdx);

    logger->debug("Starting executor thread {}", id);

    // Listen to bind queue by default
    currentQueue = scheduler.getBindQueue();
}

void FaabricExecutor::bindToFunction(const faabric::Message& msg, bool force)
{
    // If already bound, will be an error, unless forced to rebind to the same
    // message
    if (_isBound) {
        if (force) {
            if (msg.user() != boundMessage.user() ||
                msg.function() != boundMessage.function()) {
                throw std::runtime_error(
                  "Cannot force bind to a different function");
            }
        } else {
            throw std::runtime_error(
              "Cannot bind worker thread more than once");
        }
    }

    boundMessage = msg;
    _isBound = true;

    // Get queue from the scheduler
    currentQueue = scheduler.getFunctionQueue(msg);

    // Hook
    this->postBind(msg, force);
}

bool FaabricExecutor::isBound()
{
    return _isBound;
}

void FaabricExecutor::finish()
{
    if (_isBound) {
        // Notify scheduler if this thread was bound to a function
        scheduler.notifyFaasletFinished(boundMessage);
    }

    // Hook
    this->postFinish();
}

void FaabricExecutor::finishCall(faabric::Message& msg,
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

void FaabricExecutor::run()
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

    // Wait for next message
    while (true) {
        try {
            logger->debug("{} waiting for next message", this->id);
            std::string errorMessage = this->processNextMessage();

            // Drop out if there's some issue
            if (!errorMessage.empty()) {
                break;
            }
        } catch (faabric::util::ExecutorFinishedException& e) {
            // Executor has notified us it's finished
            logger->debug("{} finished", this->id);
            break;
        } catch (faabric::util::QueueTimeoutException& e) {
            // At this point we've received no message, so die off
            logger->debug("{} got no messages. Finishing", this->id);
            break;
        }
    }

    this->finish();
}

std::string FaabricExecutor::processNextMessage()
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

    // Work out which timeout
    int timeoutMs;
    faabric::util::SystemConfig conf = faabric::util::getSystemConfig();
    if (_isBound) {
        timeoutMs = conf.boundTimeout;
    } else {
        timeoutMs = conf.unboundTimeout;
    }

    faabric::Message msg = currentQueue->dequeue(timeoutMs);

    std::string errorMessage;
    if (msg.type() == faabric::Message_MessageType_FLUSH) {
        flush();

    } else if (msg.type() == faabric::Message_MessageType_BIND) {
        const std::string funcStr = faabric::util::funcToString(msg, false);
        logger->debug("{} binding to {}", id, funcStr);

        try {
            this->bindToFunction(msg);
        } catch (faabric::util::InvalidFunctionException& e) {
            errorMessage = "Invalid function: " + funcStr;
        }
    } else {
        // Do the actual execution
        errorMessage = this->executeCall(msg);
    }

    return errorMessage;
}

std::vector<std::future<int32_t>> FaabricExecutor::batchExecuteThreads(
  faabric::BatchExecuteRequest& req)
{
    const auto& logger = faabric::util::getLogger();

    const faabric::Message& firstMsg = req.messages().at(0);

    const std::string funcStr = faabric::util::funcToString(firstMsg, false);
    logger->info(
      "Batch executing {} threads of {}", req.messages().size(), funcStr);

    std::vector<std::future<int32_t>> threadFutures;
    for (const faabric::Message& msg : req.messages()) {
        std::promise<int32_t> p;
        std::future<int32_t> f = p.get_future();

        int threadPoolIdx = msg.appindex() % threadPoolSize;
        ThreadTaskPair taskPair = std::make_pair(std::move(p), &msg);
        threadQueues[threadPoolIdx].enqueue(taskPair);

        if (threads.count(threadPoolIdx) == 0) {
            // Get mutex and re-check
            faabric::util::UniqueLock lock(threadsMutex);

            if (threads.count(threadPoolIdx) == 0) {
                threads.emplace(
                  std::make_pair(threadPoolIdx, [this, threadPoolIdx] {
                      auto logger = faabric::util::getLogger();

                      for (;;) {
                          ThreadTaskPair taskPair =
                            threadQueues[threadPoolIdx].dequeue();

                          if (taskPair.second->type() ==
                              faabric::Message::KILL) {
                              taskPair.first.set_value(0);
                              logger->debug("Executor thread {} shutting down",
                                            threadPoolIdx);
                              break;
                          }

                          doBatchExecuteThread(threadPoolIdx, taskPair.second);

                          // Caller has to notify scheduler when finished
                          // executing a thread locally
                          auto& sch = faabric::scheduler::getScheduler();
                          sch.notifyCallFinished(*taskPair.second);
                      }
                  }));
            }
        }

        threadFutures.emplace_back(f);
    }

    return threadFutures;
}

std::string FaabricExecutor::executeCall(faabric::Message& call)
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

    const std::string funcStr = faabric::util::funcToString(call, true);
    logger->info("Faaslet executing {}", funcStr);

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

bool FaabricExecutor::doExecute(faabric::Message& msg)
{
    return true;
}

void FaabricExecutor::postBind(const faabric::Message& msg, bool force) {}

void FaabricExecutor::preFinishCall(faabric::Message& call,
                                    bool success,
                                    const std::string& errorMsg)
{}

void FaabricExecutor::postFinishCall() {}

void FaabricExecutor::postFinish() {}

void FaabricExecutor::flush() {}

}
