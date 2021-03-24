#include "faabric/util/func.h"
#include <faabric/scheduler/Scheduler.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/util/environment.h>
#include <faabric/util/logging.h>
#include <faabric/util/random.h>
#include <faabric/util/timing.h>

#define FLUSH_TIMEOUT_MS 10000

using namespace faabric::util;

namespace faabric::scheduler {

Scheduler::Scheduler()
  : thisHost(faabric::util::getSystemConfig().endpointHost)
  , conf(faabric::util::getSystemConfig())
  , isTestMode(false)
{
    bindQueue = std::make_shared<InMemoryMessageQueue>();

    // Set up the initial resources
    int cores = faabric::util::getUsableCores();
    thisHostResources.set_cores(cores);
}

void Scheduler::addHostToGlobalSet(const std::string& host)
{
    redis::Redis& redis = redis::Redis::getQueue();
    redis.sadd(AVAILABLE_HOST_SET, host);
}

void Scheduler::addHostToGlobalSet()
{
    redis::Redis& redis = redis::Redis::getQueue();
    redis.sadd(AVAILABLE_HOST_SET, thisHost);
}

void Scheduler::reset()
{
    // Reset queue map
    for (const auto& iter : queueMap) {
        iter.second->reset();
    }
    queueMap.clear();

    // Ensure host is set correctly
    thisHost = faabric::util::getSystemConfig().endpointHost;

    // Clear queues
    bindQueue->reset();

    // Reset scheduler state
    thisHostResources = faabric::HostResources();
    faasletCounts.clear();
    inFlightCounts.clear();

    // Records
    setTestMode(false);
    recordedMessagesAll.clear();
    recordedMessagesLocal.clear();
    recordedMessagesShared.clear();
}

void Scheduler::shutdown()
{
    reset();

    redis::Redis& redis = redis::Redis::getQueue();
    redis.srem(AVAILABLE_HOST_SET, thisHost);
}

long Scheduler::getFunctionInFlightCount(const faabric::Message& msg)
{
    const std::string funcStr = faabric::util::funcToString(msg, false);
    return inFlightCounts[funcStr];
}

std::shared_ptr<InMemoryMessageQueue> Scheduler::getFunctionQueue(
  const faabric::Message& msg)
{
    std::string funcStr = faabric::util::funcToString(msg, false);

    // This will be called from within something holding the lock
    if (queueMap.count(funcStr) == 0) {
        if (queueMap.count(funcStr) == 0) {
            auto mq = new InMemoryMessageQueue();
            queueMap.emplace(InMemoryMessageQueuePair(funcStr, mq));
        }
    }

    return queueMap[funcStr];
}

void Scheduler::notifyCallFinished(const faabric::Message& msg)
{
    faabric::util::FullLock lock(mx);

    const std::string funcStr = faabric::util::funcToString(msg, false);

    inFlightCounts[funcStr] = std::max(inFlightCounts[funcStr] - 1, 0L);

    int newInFlight =
      std::max<int>(thisHostResources.functionsinflight() - 1, 0);
    thisHostResources.set_functionsinflight(newInFlight);
}

void Scheduler::notifyFaasletFinished(const faabric::Message& msg)
{
    faabric::util::FullLock lock(mx);
    const std::string funcStr = faabric::util::funcToString(msg, false);

    faasletCounts[funcStr] = std::max(faasletCounts[funcStr] - 1, 0L);
    int newBoundExecutors = std::max(thisHostResources.boundexecutors() - 1, 0);
    thisHostResources.set_boundexecutors(newBoundExecutors);
}

std::shared_ptr<InMemoryMessageQueue> Scheduler::getBindQueue()
{
    return bindQueue;
}

Scheduler& getScheduler()
{
    // Note that this ref is shared between all faaslets on the given host
    static Scheduler scheduler;
    return scheduler;
}

std::vector<std::string> Scheduler::callFunctions(
  faabric::BatchExecuteRequest& req,
  bool forceLocal)
{
    auto logger = faabric::util::getLogger();

    int nMessages = req.messages_size();
    std::vector<std::string> executed(nMessages);

    const faabric::Message& firstMsg = req.messages().at(0);
    std::string funcStr = faabric::util::funcToString(firstMsg, false);

    auto funcQueue = this->getFunctionQueue(firstMsg);

    // Handle forced local execution
    if (forceLocal) {
        logger->debug("Executing {} functions locally", nMessages);

        for (int i = 0; i < nMessages; i++) {
            faabric::Message msg = req.messages().at(i);

            funcQueue->enqueue(msg);
            incrementInFlightCount(msg);

            executed.at(i) = thisHost;
        }
    } else {
        // If not forced local, here we need to determine which functions to
        // execute locally, and which to distribute
        if (req.masterhost().empty()) {
            throw std::runtime_error(
              "Master host for bulk execute cannot be empty");
        }

        // TODO - more fine-grained locking
        // Lock the whole scheduler to be safe
        faabric::util::FullLock lock(mx);

        // If we're not the master host, we need to forward the request back to
        // the master host. This will only happen if a nested batch execution
        // happens.
        if (req.masterhost() != thisHost) {
            if (isTestMode) {
                logger->debug("Not forwarding request to master in test mode");
            } else {
                FunctionCallClient c(req.masterhost());
                c.executeFunctions(req);
            }
        } else {
            // At this point we know we're the master host, and we've not been
            // asked to force full local execution.

            // Work out how many we can handle locally
            int cores = thisHostResources.cores();
            int available = cores - thisHostResources.functionsinflight();
            available = std::max<int>(available, 0);
            int nLocally = std::min<int>(available, nMessages);

            // Keep track of what's been done
            int remainder = nMessages;
            int nextMsgIdx = 0;

            // Build list of messages to be executed locally
            for (; nextMsgIdx < nLocally; nextMsgIdx++) {
                faabric::Message msg = req.messages().at(nextMsgIdx);

                if (req.type() == req.THREADS) {
                    // For threads we don't actually execute the functions,
                    // leaving the caller to do it
                    logger->debug("Skipping for local thread execution");
                } else {
                    funcQueue->enqueue(msg);
                    incrementInFlightCount(msg);

                    executed.at(nextMsgIdx) = thisHost;
                }

                // Record that we've dealt with one
                remainder--;
                incrementInFlightCount(msg);
            }

            // If some are left, we need to distribute
            if (remainder > 0) {
                // At this point we have a remainder, so we need to distribute
                // the rest Get the list of registered hosts for this function
                std::set<std::string>& thisRegisteredHosts =
                  registeredHosts[funcStr];

                // Schedule the remainder on these other hosts
                for (auto& h : thisRegisteredHosts) {
                    int nOnThisHost =
                      scheduleFunctionsOnHost(h, req, executed, nextMsgIdx);

                    remainder -= nOnThisHost;
                    if (remainder <= 0) {
                        break;
                    }
                }
            }

            // Now we need to find hosts that aren't already registered for
            // this function and add them
            if (remainder > 0) {
                std::set<std::string>& thisRegisteredHosts =
                  registeredHosts[funcStr];

                redis::Redis& redis = redis::Redis::getQueue();
                std::unordered_set<std::string> allHosts =
                  redis.smembers(AVAILABLE_HOST_SET);
                for (auto& h : allHosts) {
                    // Skip if already registered
                    if (thisRegisteredHosts.find(h) !=
                        thisRegisteredHosts.end()) {
                        continue;
                    }

                    // Schedule functions on this host
                    int nOnThisHost =
                      scheduleFunctionsOnHost(h, req, executed, nextMsgIdx);
                    remainder -= nOnThisHost;
                    if (remainder <= 0) {
                        break;
                    }
                }
            }

            // At this point there's no more capacity in the system, so we
            // just need to execute locally
            if (remainder > 0) {
                for (; nextMsgIdx < nMessages; nextMsgIdx++) {
                    // Flag that this message was not executed
                    executed.at(nextMsgIdx) = false;
                }
            }
        }
    }

    // Accounting
    for (int i = 0; i < nMessages; i++) {
        std::string executedHost = executed.at(i);
        faabric::Message msg = req.messages().at(i);

        // Mark if this is the first scheduling decision made on this message
        if (msg.scheduledhost().empty()) {
            msg.set_scheduledhost(executedHost);
        }

        if (!executedHost.empty() && executedHost != thisHost) {
            // Increment the number of hops
            msg.set_hops(msg.hops() + 1);
        }

        // Log results if in test mode
        if (isTestMode) {
            recordedMessagesAll.push_back(msg.id());
            if (executedHost.empty() || executedHost == thisHost) {
                recordedMessagesLocal.push_back(msg.id());
            } else {
                recordedMessagesShared.emplace_back(executedHost, msg.id());
            }
        }
    }

    return executed;
}

int Scheduler::scheduleFunctionsOnHost(const std::string& host,
                                       faabric::BatchExecuteRequest& req,
                                       std::vector<std::string>& records,
                                       int offset)
{
    int nMessages = req.messages_size();
    int remainder = nMessages - offset;

    // Execute as many as possible to this host
    faabric::HostResources r = getHostResources(host);
    int available = r.cores() - r.functionsinflight();

    // Drop out if none available
    if (available < 0) {
        return 0;
    }

    int nOnThisHost = std::min<int>(available, remainder);
    std::vector<faabric::Message> thisHostMsgs;
    for (int i = offset; i < (offset + nOnThisHost); i++) {
        thisHostMsgs.push_back(req.messages().at(i));
        records.at(i) = host;
    }

    FunctionCallClient c(host);
    faabric::BatchExecuteRequest hostRequest =
      faabric::util::batchExecFactory(thisHostMsgs);
    hostRequest.set_masterhost(req.masterhost());
    hostRequest.set_snapshotkey(req.snapshotkey());
    hostRequest.set_snapshotsize(req.snapshotsize());
    hostRequest.set_type(req.type());

    c.executeFunctions(hostRequest);

    return nOnThisHost;
}

void Scheduler::callFunction(faabric::Message& msg, bool forceLocal)
{
    // TODO - avoid this copy
    faabric::Message msgCopy = msg;
    std::vector<faabric::Message> msgs = { msg };
    faabric::BatchExecuteRequest req = faabric::util::batchExecFactory(msgs);

    // TODO - remove calls to callFunction, instead convert everything to
    // calling callFunctions directly
    std::vector<std::string> executed = callFunctions(req, forceLocal);
    if (executed.at(0).empty()) {
        callFunctions(req, true);
    }
}

void Scheduler::setTestMode(bool val)
{
    isTestMode = val;
}

std::vector<unsigned int> Scheduler::getRecordedMessagesAll()
{
    return recordedMessagesAll;
}

std::vector<unsigned int> Scheduler::getRecordedMessagesLocal()
{
    return recordedMessagesLocal;
}

std::vector<std::pair<std::string, unsigned int>>
Scheduler::getRecordedMessagesShared()
{
    return recordedMessagesShared;
}

void Scheduler::incrementInFlightCount(const faabric::Message& msg)
{
    auto logger = faabric::util::getLogger();

    // Increment the in-flight count
    const std::string funcStr = faabric::util::funcToString(msg, false);
    inFlightCounts[funcStr]++;
    thisHostResources.set_functionsinflight(
      thisHostResources.functionsinflight() + 1);

    // If we've not got one faaslet per in-flight function, add one
    int nFaaslets = faasletCounts[funcStr];
    int inFlightCount = inFlightCounts[funcStr];
    bool needToScale = nFaaslets < inFlightCount;

    if (needToScale) {
        logger->debug("Scaling up {} to {} faaslet", funcStr, nFaaslets + 1);

        // Increment faaslet count
        faasletCounts[funcStr]++;
        thisHostResources.set_boundexecutors(
          thisHostResources.boundexecutors() + 1);

        // Send bind message (i.e. request a new faaslet bind to this func)
        faabric::Message bindMsg =
          faabric::util::messageFactory(msg.user(), msg.function());
        bindMsg.set_type(faabric::Message_MessageType_BIND);
        bindMsg.set_ispython(msg.ispython());
        bindMsg.set_istypescript(msg.istypescript());
        bindMsg.set_pythonuser(msg.pythonuser());
        bindMsg.set_pythonfunction(msg.pythonfunction());
        bindMsg.set_issgx(msg.issgx());

        bindQueue->enqueue(msg);
    }
}

std::string Scheduler::getThisHost()
{
    return thisHost;
}

void Scheduler::broadcastFlush()
{
    // Get all hosts
    redis::Redis& redis = redis::Redis::getQueue();
    std::unordered_set<std::string> allHosts =
      redis.smembers(AVAILABLE_HOST_SET);

    // Remove this host from the set
    allHosts.erase(thisHost);

    // Dispatch flush message to all other hosts
    for (auto& otherHost : allHosts) {
        FunctionCallClient c(otherHost);
        c.sendFlush();
    }

    // Perform flush locally
    flushLocally();
}

void Scheduler::flushLocally()
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();
    logger->info("Flushing host {}",
                 faabric::util::getSystemConfig().endpointHost);

    // Notify all warm faaslets of flush
    for (const auto& p : faasletCounts) {
        if (p.second == 0) {
            continue;
        }

        // Clear any existing messages in the queue
        std::shared_ptr<InMemoryMessageQueue> queue = queueMap[p.first];
        queue->drain();

        // Dispatch a flush message for each warm faaslet
        for (int i = 0; i < p.second; i++) {
            faabric::Message msg;
            msg.set_type(faabric::Message_MessageType_FLUSH);
            queue->enqueue(msg);
        }
    }

    // Wait for flush messages to be consumed, then clear the queues
    for (const auto& p : queueMap) {
        logger->debug("Waiting for {} to drain on flush", p.first);
        p.second->waitToDrain(FLUSH_TIMEOUT_MS);
        p.second->reset();
    }
}

void Scheduler::setFunctionResult(faabric::Message& msg)
{
    redis::Redis& redis = redis::Redis::getQueue();

    // Record which host did the execution
    msg.set_executedhost(faabric::util::getSystemConfig().endpointHost);

    // Set finish timestamp
    msg.set_finishtimestamp(faabric::util::getGlobalClock().epochMillis());

    std::string key = msg.resultkey();
    if (key.empty()) {
        throw std::runtime_error("Result key empty. Cannot publish result");
    }

    // Write the successful result to the result queue
    std::vector<uint8_t> inputData = faabric::util::messageToBytes(msg);
    redis.enqueueBytes(key, inputData);

    // Set the result key to expire
    redis.expire(key, RESULT_KEY_EXPIRY);

    // Set long-lived result for function too
    redis.set(msg.statuskey(), inputData);
    redis.expire(key, STATUS_KEY_EXPIRY);
}

faabric::Message Scheduler::getFunctionResult(unsigned int messageId,
                                              int timeoutMs)
{
    if (messageId == 0) {
        throw std::runtime_error("Must provide non-zero message ID");
    }

    redis::Redis& redis = redis::Redis::getQueue();

    bool isBlocking = timeoutMs > 0;

    std::string resultKey = faabric::util::resultKeyFromMessageId(messageId);

    faabric::Message msgResult;

    if (isBlocking) {
        // Blocking version will throw an exception when timing out which is
        // handled by the caller.
        std::vector<uint8_t> result = redis.dequeueBytes(resultKey, timeoutMs);
        msgResult.ParseFromArray(result.data(), (int)result.size());
    } else {
        // Non-blocking version will tolerate empty responses, therefore we
        // handle the exception here
        std::vector<uint8_t> result;
        try {
            result = redis.dequeueBytes(resultKey, timeoutMs);
        } catch (redis::RedisNoResponseException& ex) {
            // Ok for no response when not blocking
        }

        if (result.empty()) {
            // Empty result has special type
            msgResult.set_type(faabric::Message_MessageType_EMPTY);
        } else {
            // Normal response if we get something from redis
            msgResult.ParseFromArray(result.data(), (int)result.size());
        }
    }

    return msgResult;
}

std::string Scheduler::getMessageStatus(unsigned int messageId)
{
    const faabric::Message result = getFunctionResult(messageId, 0);

    if (result.type() == faabric::Message_MessageType_EMPTY) {
        return "RUNNING";
    } else if (result.returnvalue() == 0) {
        return "SUCCESS: " + result.outputdata();
    } else {
        return "FAILED: " + result.outputdata();
    }
}

faabric::HostResources Scheduler::getThisHostResources()
{
    return thisHostResources;
}

faabric::HostResources Scheduler::getHostResources(const std::string& host)
{
    // Get the resources for that host
    faabric::ResourceRequest resourceReq;
    FunctionCallClient c(host);

    faabric::HostResources resp = c.getResources(resourceReq);

    return resp;
}

// --------------------------------------------
// EXECUTION GRAPH
// --------------------------------------------

#define CHAINED_SET_PREFIX "chained_"
std::string getChainedKey(unsigned int msgId)
{
    return std::string(CHAINED_SET_PREFIX) + std::to_string(msgId);
}

void Scheduler::logChainedFunction(unsigned int parentMessageId,
                                   unsigned int chainedMessageId)
{
    redis::Redis& redis = redis::Redis::getQueue();

    const std::string& key = getChainedKey(parentMessageId);
    redis.sadd(key, std::to_string(chainedMessageId));
    redis.expire(key, STATUS_KEY_EXPIRY);
}

std::unordered_set<unsigned int> Scheduler::getChainedFunctions(
  unsigned int msgId)
{
    redis::Redis& redis = redis::Redis::getQueue();

    const std::string& key = getChainedKey(msgId);
    const std::unordered_set<std::string> chainedCalls = redis.smembers(key);

    std::unordered_set<unsigned int> chainedIds;
    for (auto i : chainedCalls) {
        chainedIds.insert(std::stoi(i));
    }

    return chainedIds;
}

ExecGraph Scheduler::getFunctionExecGraph(unsigned int messageId)
{
    ExecGraphNode rootNode = getFunctionExecGraphNode(messageId);
    ExecGraph graph{ .rootNode = rootNode };

    return graph;
}

ExecGraphNode Scheduler::getFunctionExecGraphNode(unsigned int messageId)
{
    redis::Redis& redis = redis::Redis::getQueue();

    // Get the result for this message
    std::string statusKey = faabric::util::statusKeyFromMessageId(messageId);
    std::vector<uint8_t> messageBytes = redis.get(statusKey);
    faabric::Message result;
    result.ParseFromArray(messageBytes.data(), (int)messageBytes.size());

    // Recurse through chained calls
    std::unordered_set<unsigned int> chainedMsgIds =
      getChainedFunctions(messageId);
    std::vector<ExecGraphNode> children;
    for (auto c : chainedMsgIds) {
        children.emplace_back(getFunctionExecGraphNode(c));
    }

    // Build the node
    ExecGraphNode node{ .msg = result, .children = children };

    return node;
}

}
