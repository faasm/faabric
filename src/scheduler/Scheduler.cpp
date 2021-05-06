#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/scheduler/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/random.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/testing.h>
#include <faabric/util/timing.h>

#include <unordered_set>

#define FLUSH_TIMEOUT_MS 10000

using namespace faabric::util;

namespace faabric::scheduler {

int decrementAboveZero(int input)
{
    return std::max<int>(input - 1, 0);
}

Scheduler::Scheduler()
  : thisHost(faabric::util::getSystemConfig().endpointHost)
  , conf(faabric::util::getSystemConfig())
{
    bindQueue = std::make_shared<InMemoryMessageQueue>();

    // Set up the initial resources
    int cores = faabric::util::getUsableCores();
    thisHostResources.set_cores(cores);
}

std::unordered_set<std::string> Scheduler::getAvailableHosts()
{
    redis::Redis& redis = redis::Redis::getQueue();
    return redis.smembers(AVAILABLE_HOST_SET);
}

void Scheduler::addHostToGlobalSet(const std::string& host)
{
    redis::Redis& redis = redis::Redis::getQueue();
    redis.sadd(AVAILABLE_HOST_SET, host);
}

void Scheduler::removeHostFromGlobalSet(const std::string& host)
{
    redis::Redis& redis = redis::Redis::getQueue();
    redis.srem(AVAILABLE_HOST_SET, host);
}

void Scheduler::addHostToGlobalSet()
{
    redis::Redis& redis = redis::Redis::getQueue();
    redis.sadd(AVAILABLE_HOST_SET, thisHost);
}

void Scheduler::closeFunctionCallClients()
{
    // Close function call clients
    for (auto& iter : functionCallClients) {
        iter.second.close();
    }
    functionCallClients.clear();
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

    // Reset resources
    thisHostResources = faabric::HostResources();
    thisHostResources.set_cores(faabric::util::getUsableCores());

    // Reset scheduler state
    registeredHosts.clear();
    faasletCounts.clear();
    inFlightCounts.clear();

    // Records
    recordedMessagesAll.clear();
    recordedMessagesLocal.clear();
    recordedMessagesShared.clear();

    closeFunctionCallClients();
}

void Scheduler::shutdown()
{
    reset();

    removeHostFromGlobalSet(thisHost);
}

long Scheduler::getFunctionInFlightCount(const faabric::Message& msg)
{
    const std::string funcStr = faabric::util::funcToString(msg, false);
    return inFlightCounts[funcStr];
}

long Scheduler::getFunctionFaasletCount(const faabric::Message& msg)
{
    const std::string funcStr = faabric::util::funcToString(msg, false);
    return faasletCounts[funcStr];
}

int Scheduler::getFunctionRegisteredHostCount(const faabric::Message& msg)
{
    const std::string funcStr = faabric::util::funcToString(msg, false);
    return (int)registeredHosts[funcStr].size();
}

std::unordered_set<std::string> Scheduler::getFunctionRegisteredHosts(
  const faabric::Message& msg)
{
    const std::string funcStr = faabric::util::funcToString(msg, false);
    return registeredHosts[funcStr];
}

void Scheduler::removeRegisteredHost(const std::string& host,
                                     const faabric::Message& msg)
{
    const std::string funcStr = faabric::util::funcToString(msg, false);
    registeredHosts[funcStr].erase(host);
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

    inFlightCounts[funcStr] = decrementAboveZero(inFlightCounts[funcStr]);

    int newInFlight = decrementAboveZero(thisHostResources.functionsinflight());
    thisHostResources.set_functionsinflight(newInFlight);
}

void Scheduler::notifyFaasletFinished(const faabric::Message& msg)
{
    faabric::util::FullLock lock(mx);
    const std::string funcStr = faabric::util::funcToString(msg, false);

    auto logger = faabric::util::getLogger();
    // Unregister this host if not master and no more faaslets assigned to
    // this function
    if (msg.masterhost().empty()) {
        logger->error("Message {} without master host set", funcStr);
        throw std::runtime_error("Message without master host");
    }

    // Update the faaslet count
    int oldCount = faasletCounts[funcStr];
    if (oldCount > 0) {
        int newCount = faasletCounts[funcStr] - 1;
        faasletCounts[funcStr] = newCount;

        // Unregister if this was the last faaslet for that function
        bool isMaster = thisHost == msg.masterhost();
        if (newCount == 0 && !isMaster) {
            faabric::UnregisterRequest req;
            req.set_host(thisHost);
            *req.mutable_function() = msg;

            getFunctionCallClient(msg.masterhost()).unregister(req);
        }
    }

    // Update bound executors on this host
    int newBoundExecutors =
      decrementAboveZero(thisHostResources.boundexecutors());
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
    bool isThreads = req.type() == req.THREADS;
    std::vector<std::string> executed(nMessages);

    // Note, we assume all the messages are for the same function and master
    // host
    const faabric::Message& firstMsg = req.messages().at(0);
    std::string funcStr = faabric::util::funcToString(firstMsg, false);
    std::string masterHost = firstMsg.masterhost();
    if (masterHost.empty()) {
        std::string funcStrWithId = faabric::util::funcToString(firstMsg, true);
        logger->error("Request {} has no master host", funcStrWithId);
        throw std::runtime_error("Message with no master host");
    }

    // For threads/ processes we need to have a snapshot key and be ready to
    // push the snapshot to other hosts
    faabric::util::SnapshotData snapshotData;
    std::string snapshotKey = firstMsg.snapshotkey();
    bool snapshotNeeded =
      req.type() == req.THREADS || req.type() == req.PROCESSES;
    if (snapshotNeeded) {
        if (snapshotKey.empty()) {
            logger->error("No snapshot provided for {}", funcStr);
            throw std::runtime_error(
              "Empty snapshot for distributed threads/ processes");
        }

        faabric::snapshot::SnapshotRegistry& reg =
          faabric::snapshot::getSnapshotRegistry();
        snapshotData = reg.getSnapshot(snapshotKey);
    }

    auto funcQueue = this->getFunctionQueue(firstMsg);

    // TODO - more fine-grained locking. This blocks all functions
    // Lock the whole scheduler to be safe
    faabric::util::FullLock lock(mx);

    // Handle forced local execution
    if (forceLocal) {
        logger->debug("Executing {} x {} locally", nMessages, funcStr);

        for (int i = 0; i < nMessages; i++) {
            faabric::Message msg = req.messages().at(i);

            funcQueue->enqueue(msg);
            incrementInFlightCount(msg);
            addFaaslets(msg);

            executed.at(i) = thisHost;
        }
    } else {
        // If we're not the master host, we need to forward the request back to
        // the master host. This will only happen if a nested batch execution
        // happens.
        if (masterHost != thisHost) {
            logger->debug("Forwarding {} {} back to master {}",
                          nMessages,
                          funcStr,
                          masterHost);

            getFunctionCallClient(masterHost).executeFunctions(req);
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

            if (isThreads && nLocally > 0) {
                logger->debug("Returning {} of {} {} for local threads",
                              nLocally,
                              nMessages,
                              funcStr);
            } else if (nLocally > 0) {
                logger->debug("Executing {} of {} {} locally",
                              nLocally,
                              nMessages,
                              funcStr);
            } else {
                logger->debug("No local capacity, distributing {} x {}",
                              nMessages,
                              funcStr);
            }

            // Build list of messages to be executed locally
            for (; nextMsgIdx < nLocally; nextMsgIdx++) {
                faabric::Message msg = req.messages().at(nextMsgIdx);

                remainder--;
                incrementInFlightCount(msg);

                // Provided we're not executing threads, execute the functions
                // now
                if (!isThreads) {
                    funcQueue->enqueue(msg);
                    executed.at(nextMsgIdx) = thisHost;
                    addFaaslets(msg);
                }
            }

            // If some are left, we need to distribute
            if (remainder > 0) {
                // At this point we have a remainder, so we need to distribute
                // the rest Get the list of registered hosts for this function
                std::unordered_set<std::string>& thisRegisteredHosts =
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
                std::unordered_set<std::string>& thisRegisteredHosts =
                  registeredHosts[funcStr];

                std::unordered_set<std::string> allHosts = getAvailableHosts();

                std::unordered_set<std::string> targetHosts;

                // Build list of target hosts
                for (auto& h : allHosts) {
                    // Skip if already registered
                    if (thisRegisteredHosts.find(h) !=
                        thisRegisteredHosts.end()) {
                        continue;
                    }

                    // Skip this host
                    if (h == thisHost) {
                        continue;
                    }

                    targetHosts.insert(h);
                }

                for (auto& h : targetHosts) {
                    // Schedule functions on this host
                    int nOnThisHost =
                      scheduleFunctionsOnHost(h, req, executed, nextMsgIdx);

                    remainder -= nOnThisHost;

                    // Register the host if it's exected a function
                    if (nOnThisHost > 0) {
                        logger->debug("Registering {} for {}", h, funcStr);
                        thisRegisteredHosts.insert(h);
                    }

                    // Stop if we've scheduled all functions
                    if (remainder <= 0) {
                        break;
                    }
                }
            }

            // At this point there's no more capacity in the system, so we
            // just need to execute locally
            if (remainder > 0) {
                for (; nextMsgIdx < nMessages; nextMsgIdx++) {
                    faabric::Message msg = req.messages().at(nextMsgIdx);
                    incrementInFlightCount(msg);

                    if (isThreads) {
                        logger->warn(
                          "No capacity for {} thread, returning to caller",
                          funcStr);
                        executed.at(nextMsgIdx) = "";
                    } else {
                        logger->warn("No capacity for {}, executing locally",
                                     funcStr);

                        funcQueue->enqueue(msg);
                        executed.at(nextMsgIdx) = thisHost;
                        addFaaslets(msg);
                    }
                }
            }
        }
    }

    // Accounting
    for (int i = 0; i < nMessages; i++) {
        std::string executedHost = executed.at(i);
        faabric::Message msg = req.messages().at(i);

        // Log results if in test mode
        if (faabric::util::isTestMode()) {
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

void Scheduler::broadcastSnapshotDelete(const faabric::Message& msg,
                                        const std::string& snapshotKey)
{

    std::string funcStr = faabric::util::funcToString(msg, false);
    std::unordered_set<std::string>& thisRegisteredHosts =
      registeredHosts[funcStr];

    for (auto host : thisRegisteredHosts) {
        SnapshotClient c(host);
        c.deleteSnapshot(snapshotKey);
    }
}

int Scheduler::scheduleFunctionsOnHost(const std::string& host,
                                       faabric::BatchExecuteRequest& req,
                                       std::vector<std::string>& records,
                                       int offset)
{
    auto logger = faabric::util::getLogger();
    faabric::Message firstMsg = req.messages().at(0);
    std::string funcStr = faabric::util::funcToString(firstMsg, false);

    int nMessages = req.messages_size();
    int remainder = nMessages - offset;

    // Execute as many as possible to this host
    faabric::HostResources r = getHostResources(host);
    int available = r.cores() - r.functionsinflight();

    // Drop out if none available
    if (available <= 0) {
        logger->debug("Not scheduling {} on {}, no resources", funcStr, host);
        return 0;
    }

    int nOnThisHost = std::min<int>(available, remainder);
    std::vector<faabric::Message> thisHostMsgs;
    for (int i = offset; i < (offset + nOnThisHost); i++) {
        thisHostMsgs.push_back(req.messages().at(i));
        records.at(i) = host;
    }

    // Push the snapshot if necessary
    if (req.type() == req.THREADS || req.type() == req.PROCESSES) {
        std::string snapshotKey = firstMsg.snapshotkey();
        SnapshotClient c(host);
        const SnapshotData& d =
          snapshot::getSnapshotRegistry().getSnapshot(snapshotKey);
        c.pushSnapshot(snapshotKey, d);
    }

    logger->debug(
      "Sending {} of {} {} to {}", nOnThisHost, nMessages, funcStr, host);

    faabric::BatchExecuteRequest hostRequest =
      faabric::util::batchExecFactory(thisHostMsgs);
    hostRequest.set_snapshotkey(req.snapshotkey());
    hostRequest.set_snapshotsize(req.snapshotsize());
    hostRequest.set_type(req.type());

    getFunctionCallClient(host).executeFunctions(hostRequest);

    return nOnThisHost;
}

void Scheduler::callFunction(faabric::Message& msg, bool forceLocal)
{
    // TODO - avoid this copy
    faabric::Message msgCopy = msg;
    std::vector<faabric::Message> msgs = { msg };
    faabric::BatchExecuteRequest req = faabric::util::batchExecFactory(msgs);

    // Specify that this is a normal function, not a thread
    req.set_type(req.FUNCTIONS);

    // Make the call
    callFunctions(req, forceLocal);
}

std::vector<unsigned int> Scheduler::getRecordedMessagesAll()
{
    return recordedMessagesAll;
}

std::vector<unsigned int> Scheduler::getRecordedMessagesLocal()
{
    return recordedMessagesLocal;
}

FunctionCallClient& Scheduler::getFunctionCallClient(
  const std::string& otherHost)
{
    auto it = functionCallClients.find(otherHost);
    if (it == functionCallClients.end()) {
        auto _it = functionCallClients.try_emplace(otherHost, otherHost);
        if (!_it.second) {
            throw std::runtime_error("Error inserting function call client");
        }
        it = _it.first;
    }
    return it->second;
}

std::vector<std::pair<std::string, unsigned int>>
Scheduler::getRecordedMessagesShared()
{
    return recordedMessagesShared;
}

void Scheduler::incrementInFlightCount(const faabric::Message& msg)
{
    const std::string funcStr = faabric::util::funcToString(msg, false);
    inFlightCounts[funcStr]++;
    thisHostResources.set_functionsinflight(
      thisHostResources.functionsinflight() + 1);
}

void Scheduler::addFaaslets(const faabric::Message& msg)
{
    auto logger = faabric::util::getLogger();
    const std::string funcStr = faabric::util::funcToString(msg, false);

    // If we've not got one faaslet per in-flight function, add one
    int nFaaslets = faasletCounts[funcStr];
    int inFlightCount = inFlightCounts[funcStr];
    bool needToScale = nFaaslets < inFlightCount;

    if (needToScale) {
        logger->debug(
          "Scaling {} {}->{} faaslets", funcStr, nFaaslets, nFaaslets + 1);

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

        bindQueue->enqueue(bindMsg);
    }
}

std::string Scheduler::getThisHost()
{
    return thisHost;
}

void Scheduler::broadcastFlush()
{
    // Get all hosts
    std::unordered_set<std::string> allHosts = getAvailableHosts();

    // Remove this host from the set
    allHosts.erase(thisHost);

    // Dispatch flush message to all other hosts
    for (auto& otherHost : allHosts) {
        getFunctionCallClient(otherHost).sendFlush();
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

void Scheduler::setThisHostResources(faabric::HostResources& res)
{
    thisHostResources = res;
}

faabric::HostResources Scheduler::getHostResources(const std::string& host)
{
    // Get the resources for that host
    faabric::ResourceRequest resourceReq;
    return getFunctionCallClient(host).getResources(resourceReq);
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
