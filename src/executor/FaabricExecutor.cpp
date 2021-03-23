#include "faabric/util/queue.h"
#include <faabric/executor/FaabricExecutor.h>

#include <faabric/state/State.h>
#include <faabric/util/config.h>

namespace faabric::executor {
FaabricExecutor::FaabricExecutor(int threadIdxIn)
  : threadIdx(threadIdxIn)
  , scheduler(faabric::scheduler::getScheduler())
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

    // Set an ID for this Faaslet
    id = faabric::util::getSystemConfig().endpointHost + "_" +
         std::to_string(threadIdx);

    logger->debug("Starting worker thread {}", id);

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
        scheduler.notifyNodeFinished(boundMessage);
        scheduler.removeBoundExecutor();
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
    scheduler.removeFunctionInFlight();

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
        logger->info("{} binding to {}", id, funcStr);

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
