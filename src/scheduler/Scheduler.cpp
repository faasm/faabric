#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/ExecutorFactory.h>
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

Scheduler& getScheduler()
{
    static Scheduler sch;
    return sch;
}

Scheduler::Scheduler()
  : thisHost(faabric::util::getSystemConfig().endpointHost)
  , conf(faabric::util::getSystemConfig())
{
    // Set up the initial resources
    int cores = faabric::util::getUsableCores();
    thisHostResources.set_slots(cores);
}

std::set<std::string> Scheduler::getAvailableHosts()
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

void Scheduler::reset()
{
    // Shut down all Executors
    for (auto p : executors) {
        for (auto e : p.second) {
            e->finish();
        }
    }

    executors.clear();

    // Ensure host is set correctly
    thisHost = faabric::util::getSystemConfig().endpointHost;

    // Reset resources
    thisHostResources = faabric::HostResources();
    thisHostResources.set_slots(faabric::util::getUsableCores());

    // Reset scheduler state
    availableHostsCache.clear();
    registeredHosts.clear();

    // Records
    recordedMessagesAll.clear();
    recordedMessagesLocal.clear();
    recordedMessagesShared.clear();
}

void Scheduler::shutdown()
{
    reset();

    removeHostFromGlobalSet(thisHost);
}

long Scheduler::getFunctionExecutorCount(const faabric::Message& msg)
{
    const std::string funcStr = faabric::util::funcToString(msg, false);
    return executors[funcStr].size();
}

int Scheduler::getFunctionRegisteredHostCount(const faabric::Message& msg)
{
    const std::string funcStr = faabric::util::funcToString(msg, false);
    return (int)registeredHosts[funcStr].size();
}

std::set<std::string> Scheduler::getFunctionRegisteredHosts(
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

void Scheduler::vacateSlot()
{
    faabric::util::FullLock lock(mx);
    thisHostResources.set_usedslots(thisHostResources.usedslots() - 1);
}

void Scheduler::notifyExecutorFinished(Executor* exec,
                                       const faabric::Message& msg)
{
    faabric::util::FullLock lock(mx);

    const auto& logger = faabric::util::getLogger();
    logger->debug("{} finished message {}", exec->id, msg.id());

    // Reset this executor ready for next invocation
    exec->releaseClaim();
    exec->reset(msg);
}

void Scheduler::notifyExecutorShutdown(Executor* exec,
                                       const faabric::Message& msg)
{
    faabric::util::FullLock lock(mx);

    std::string funcStr = faabric::util::funcToString(msg, false);

    // Remove from warm executors
    std::string execId = exec->id;
    std::vector<std::shared_ptr<Executor>>& thisExecutors = executors[funcStr];
    std::remove_if(thisExecutors.begin(),
                   thisExecutors.end(),
                   [&execId](const auto& p) { return p->id == execId; });

    if (thisExecutors.empty()) {
        // Unregister if this was the last executor for that function
        bool isMaster = thisHost == msg.masterhost();
        if (!isMaster) {
            faabric::UnregisterRequest req;
            req.set_host(thisHost);
            *req.mutable_function() = msg;

            FunctionCallClient c(msg.masterhost());
            c.unregister(req);
        }
    }
}

std::vector<std::string> Scheduler::callFunctions(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  bool forceLocal)
{
    const auto& logger = faabric::util::getLogger();

    // Extract properties of the request
    int nMessages = req->messages_size();
    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;
    std::vector<std::string> executed(nMessages);

    // Note, we assume all the messages are for the same function and master
    const faabric::Message& firstMsg = req->messages().at(0);
    std::string funcStr = faabric::util::funcToString(firstMsg, false);
    std::string masterHost = firstMsg.masterhost();
    if (masterHost.empty()) {
        std::string funcStrWithId = faabric::util::funcToString(firstMsg, true);
        logger->error("Request {} has no master host", funcStrWithId);
        throw std::runtime_error("Message with no master host");
    }

    // TODO - more granular locking, this is incredibly conservative
    faabric::util::FullLock lock(mx);

    // We want to dispatch remote calls here, and record what's left to be done
    // locally
    std::vector<int> localMessageIdxs;
    if (!forceLocal && masterHost != thisHost) {
        // If we're not the master host, we need to forward the request back to
        // the master host. This will only happen if a nested batch execution
        // happens.
        logger->debug(
          "Forwarding {} {} back to master {}", nMessages, funcStr, masterHost);

        FunctionCallClient c(masterHost);
        c.executeFunctions(req);
        return executed;

    } else if (forceLocal) {
        // We're forced to execute locally here so we do all the messages
        for (int i = 0; i < nMessages; i++) {
            localMessageIdxs.emplace_back(i);
            executed.at(i) = thisHost;
        }
    } else {
        // At this point we know we're the master host, and we've not been
        // asked to force full local execution.

        // For threads/ processes we need to have a snapshot key and be
        // ready to push the snapshot to other hosts
        faabric::util::SnapshotData snapshotData;
        std::string snapshotKey = firstMsg.snapshotkey();
        bool snapshotNeeded =
          req->type() == req->THREADS || req->type() == req->PROCESSES;

        if (snapshotNeeded && snapshotKey.empty()) {
            logger->error("No snapshot provided for {}", funcStr);
            throw std::runtime_error(
              "Empty snapshot for distributed threads/ processes");
        } else if (snapshotNeeded) {
            snapshotData =
              faabric::snapshot::getSnapshotRegistry().getSnapshot(snapshotKey);
        }

        // Work out how many we can handle locally
        int nLocally;
        {
            int slots = thisHostResources.slots();

            // Work out available cores, flooring at zero
            int available = slots - thisHostResources.usedslots();
            available = std::max<int>(available, 0);

            // Claim as many as we can
            nLocally = std::min<int>(available, nMessages);
        }

        // Add those that can be executed locally
        if (nLocally > 0) {
            logger->debug(
              "Executing {}/{} {} locally", nLocally, nMessages, funcStr);
            for (int i = 0; i < nLocally; i++) {
                localMessageIdxs.emplace_back(i);
                executed.at(i) = thisHost;
            }
        }

        // If some are left, we need to distribute
        int offset = nLocally;
        if (offset < nMessages) {
            // At this point we have a remainder, so we need to distribute
            // the rest over the registered hosts for this function
            std::set<std::string>& thisRegisteredHosts =
              registeredHosts[funcStr];

            // Schedule the remainder on these other hosts
            for (auto& h : thisRegisteredHosts) {
                int nOnThisHost =
                  scheduleFunctionsOnHost(h, req, executed, offset);

                offset += nOnThisHost;
                if (offset >= nMessages) {
                    break;
                }
            }
        }

        if (offset < nMessages) {
            std::vector<std::string> unregisteredHosts =
              getUnregisteredHosts(funcStr);

            for (auto& h : unregisteredHosts) {
                // Skip if this host
                if (h == thisHost) {
                    continue;
                }

                // Schedule functions on the host
                int nOnThisHost =
                  scheduleFunctionsOnHost(h, req, executed, offset);

                // Register the host if it's exected a function
                if (nOnThisHost > 0) {
                    logger->debug("Registering {} for {}", h, funcStr);
                    registeredHosts[funcStr].insert(h);
                }

                offset += nOnThisHost;
                if (offset >= nMessages) {
                    break;
                }
            }
        }

        // At this point there's no more capacity in the system, so we
        // just need to execute locally
        if (offset < nMessages) {
            int overloadCount = nMessages - offset;
            logger->debug("Overloading {}/{} {} locally",
                          overloadCount,
                          nMessages,
                          funcStr);

            for (; offset < nMessages; offset++) {
                localMessageIdxs.emplace_back(offset);
                executed.at(offset) = thisHost;
            }
        }

        // Sanity check
        assert(offset == nMessages);
    }

    // Register thread results if necessary
    if (isThreads) {
        for (auto& m : req->messages()) {
            registerThread(m.id());
        }
    }

    // Schedule messages locally if necessary. For threads we only need one
    // executor, for anything else we want one Executor per function in flight
    if (!localMessageIdxs.empty()) {
        // Update slots
        thisHostResources.set_usedslots(thisHostResources.usedslots() +
                                        localMessageIdxs.size());

        if (isThreads) {
            // Threads use the existing executor. We assume there's only one
            // running at a time.
            std::vector<std::shared_ptr<Executor>>& thisExecutors =
              executors[funcStr];

            std::shared_ptr<Executor> e = nullptr;
            if (thisExecutors.empty()) {
                // Create executor if not exists
                e = claimExecutor(firstMsg);
            } else if (thisExecutors.size() == 1) {
                // Use existing executor if exists
                e = thisExecutors.back();
            } else {
                logger->error("Found {} executors for threaded function {}",
                              thisExecutors.size(),
                              funcStr);
                throw std::runtime_error(
                  "Expected only one executor for threaded function");
            }

            // Execute the tasks
            e->executeTasks(localMessageIdxs, req);
        } else {
            // Non-threads require one executor per task
            for (auto i : localMessageIdxs) {
                std::shared_ptr<Executor> e = claimExecutor(firstMsg);
                e->executeTasks({ i }, req);
            }
        }
    }

    // Accounting
    for (int i = 0; i < nMessages; i++) {
        std::string executedHost = executed.at(i);
        faabric::Message msg = req->messages().at(i);

        // Log results if in test mode
        if (faabric::util::isTestMode()) {
            recordedMessagesAll.push_back(msg);
            if (executedHost.empty() || executedHost == thisHost) {
                recordedMessagesLocal.push_back(msg);
            } else {
                recordedMessagesShared.emplace_back(executedHost, msg);
            }
        }
    }

    return executed;
}

std::vector<std::string> Scheduler::getUnregisteredHosts(
  const std::string funcStr,
  bool noCache)
{
    // Load the list of available hosts
    if (availableHostsCache.empty() || noCache) {
        availableHostsCache = getAvailableHosts();
    }

    // At this point we know we need to enlist unregistered hosts
    std::set<std::string>& thisRegisteredHosts = registeredHosts[funcStr];

    std::vector<std::string> unregisteredHosts;

    std::set_difference(
      availableHostsCache.begin(),
      availableHostsCache.end(),
      thisRegisteredHosts.begin(),
      thisRegisteredHosts.end(),
      std::inserter(unregisteredHosts, unregisteredHosts.begin()));

    // If we've not got any, try again without caching
    if (unregisteredHosts.empty() && !noCache) {
        return getUnregisteredHosts(funcStr, true);
    }

    return unregisteredHosts;
}

void Scheduler::broadcastSnapshotDelete(const faabric::Message& msg,
                                        const std::string& snapshotKey)
{

    std::string funcStr = faabric::util::funcToString(msg, false);
    std::set<std::string>& thisRegisteredHosts = registeredHosts[funcStr];

    for (auto host : thisRegisteredHosts) {
        SnapshotClient c(host);
        c.deleteSnapshot(snapshotKey);
    }
}

int Scheduler::scheduleFunctionsOnHost(
  const std::string& host,
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  std::vector<std::string>& records,
  int offset)
{
    auto logger = faabric::util::getLogger();
    const faabric::Message& firstMsg = req->messages().at(0);
    std::string funcStr = faabric::util::funcToString(firstMsg, false);

    int nMessages = req->messages_size();
    int remainder = nMessages - offset;

    // Execute as many as possible to this host
    faabric::HostResources r = getHostResources(host);
    int available = r.slots() - r.usedslots();

    // Drop out if none available
    if (available <= 0) {
        logger->debug("Not scheduling {} on {}, no resources", funcStr, host);
        return 0;
    }

    // Set up new request
    std::shared_ptr<faabric::BatchExecuteRequest> hostRequest =
      faabric::util::batchExecFactory();
    hostRequest->set_snapshotkey(req->snapshotkey());
    hostRequest->set_snapshotsize(req->snapshotsize());
    hostRequest->set_type(req->type());

    // Add messages
    int nOnThisHost = std::min<int>(available, remainder);
    for (int i = offset; i < (offset + nOnThisHost); i++) {
        *hostRequest->add_messages() = req->messages().at(i);
        records.at(i) = host;
    }

    // Push the snapshot if necessary
    std::string snapshotKey = firstMsg.snapshotkey();
    if (!snapshotKey.empty()) {
        SnapshotClient c(host);
        const SnapshotData& d =
          snapshot::getSnapshotRegistry().getSnapshot(snapshotKey);
        c.pushSnapshot(snapshotKey, d);
    }

    logger->debug(
      "Sending {}/{} {} to {}", nOnThisHost, nMessages, funcStr, host);

    FunctionCallClient c(host);
    c.executeFunctions(hostRequest);

    return nOnThisHost;
}

void Scheduler::callFunction(faabric::Message& msg, bool forceLocal)
{
    // TODO - avoid this copy
    auto req = faabric::util::batchExecFactory();
    *req->add_messages() = msg;

    // Specify that this is a normal function, not a thread
    req->set_type(req->FUNCTIONS);

    // Make the call
    callFunctions(req, forceLocal);
}

void Scheduler::clearRecordedMessages()
{
    recordedMessagesAll.clear();
    recordedMessagesLocal.clear();
    recordedMessagesShared.clear();
}

std::vector<faabric::Message> Scheduler::getRecordedMessagesAll()
{
    return recordedMessagesAll;
}

std::vector<faabric::Message> Scheduler::getRecordedMessagesLocal()
{
    return recordedMessagesLocal;
}

std::vector<std::pair<std::string, faabric::Message>>
Scheduler::getRecordedMessagesShared()
{
    return recordedMessagesShared;
}

std::shared_ptr<Executor> Scheduler::claimExecutor(const faabric::Message& msg)
{
    const auto& logger = faabric::util::getLogger();
    std::string funcStr = faabric::util::funcToString(msg, false);

    std::vector<std::shared_ptr<Executor>>& thisExecutors = executors[funcStr];
    int nExecutors = thisExecutors.size();

    std::shared_ptr<faabric::scheduler::ExecutorFactory> factory =
      getExecutorFactory();

    std::shared_ptr<Executor> claimed = nullptr;
    for (auto& e : thisExecutors) {
        if (e->tryClaim()) {
            claimed = e;
            logger->debug(
              "Reusing warm executor {} for {}", claimed->id, funcStr);
            break;
        }
    }

    // We have no warm executors available, so scale up
    if (claimed == nullptr) {
        logger->debug(
          "Scaling {} from {} -> {}", funcStr, nExecutors, nExecutors + 1);

        thisExecutors.emplace_back(factory->createExecutor(msg));
        claimed = thisExecutors.back();

        // Claim it
        assert(claimed->tryClaim());
    }

    return claimed;
}

std::string Scheduler::getThisHost()
{
    return thisHost;
}

void Scheduler::broadcastFlush()
{
    // Get all hosts
    std::set<std::string> allHosts = getAvailableHosts();

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

    // Flush each executor
    for (auto& p : executors) {
        for (auto& f : p.second) {
            f->flush();
        }
    }
}

void Scheduler::setFunctionResult(faabric::Message& msg)
{
    // Vacate the slot taken by this function
    vacateSlot();

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

void Scheduler::registerThread(uint32_t msgId)
{
    // Here we need to ensure the promise is registered locally so callers can
    // start waiting
    threadResults[msgId];
}

void Scheduler::setThreadResult(const faabric::Message& msg,
                                int32_t returnValue)
{
    // Vacate the slot taken by this thread
    vacateSlot();

    bool isMaster = msg.masterhost() == conf.endpointHost;
    const auto& logger = faabric::util::getLogger();

    if (isMaster) {
        setThreadResult(msg.id(), returnValue);
    } else {
        logger->debug("Sending thread result {} for {} to {}",
                      returnValue,
                      msg.id(),
                      msg.masterhost());

        FunctionCallClient c(msg.masterhost());
        faabric::ThreadResultRequest req;
        req.set_messageid(msg.id());
        req.set_returnvalue(returnValue);
        c.setThreadResult(req);
    }
}

void Scheduler::setThreadResult(uint32_t msgId, int32_t returnValue)
{
    threadResults[msgId].set_value(returnValue);
}

int32_t Scheduler::awaitThreadResult(uint32_t messageId)
{
    const auto& logger = faabric::util::getLogger();

    if (threadResults.count(messageId) == 0) {
        logger->error("Thread {} not registered on this host", messageId);
        throw std::runtime_error("Awaiting unregistered thread");
    }

    return threadResults[messageId].get_future().get();
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

std::set<unsigned int> Scheduler::getChainedFunctions(unsigned int msgId)
{
    redis::Redis& redis = redis::Redis::getQueue();

    const std::string& key = getChainedKey(msgId);
    const std::set<std::string> chainedCalls = redis.smembers(key);

    std::set<unsigned int> chainedIds;
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
    std::set<unsigned int> chainedMsgIds = getChainedFunctions(messageId);
    std::vector<ExecGraphNode> children;
    for (auto c : chainedMsgIds) {
        children.emplace_back(getFunctionExecGraphNode(c));
    }

    // Build the node
    ExecGraphNode node{ .msg = result, .children = children };

    return node;
}
}
