#pragma once

#include <faabric/scheduler/ExecGraph.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>

#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/queue.h>

#include <shared_mutex>

#define AVAILABLE_HOST_SET "available_nodes"

namespace faabric::scheduler {
// Note - default opinion when zero initialised should be maybe
enum class SchedulerOpinion
{
    MAYBE = 0,
    YES = 1,
    NO = 2,
};

struct Resources
{
    int slotsTotal = 0;
    int slotsAvailable = 0;
    int boundExecutors = 0;
    int callsInFlight = 0;
};

class Scheduler
{
  public:
    Scheduler();

    // ----------------------------------
    // External API
    // ----------------------------------
    void callFunction(faabric::Message& msg, bool forceLocal = false);

    std::vector<bool> callFunctions(faabric::BatchExecuteRequest& req,
                                    bool forceLocal = false);

    void reset();

    void shutdown();

    // ----------------------------------
    // Internal API
    // ----------------------------------
    Resources getThisHostResources();

    Resources getHostResources(const std::string& host, int slotsNeeded);

    void removeRegisteredHost(const std::string& host,
                              const faabric::Message& msg);

    void takeSlots(int n);

    void releaseSlots(int n);

    void addHostToGlobalSet(const std::string& host);

    void addHostToGlobalSet();

    void removeHostFromGlobalSet();

    void addBoundExecutor();

    void removeBoundExecutor();

    void addFunctionInFlight();

    void removeFunctionInFlight();

    // ----------------------------------
    // Legacy
    // ----------------------------------
    SchedulerOpinion getLatestOpinion(const faabric::Message& msg);

    std::string getBestHostForFunction(const faabric::Message& msg);

    void enqueueMessage(const faabric::Message& msg);

    std::shared_ptr<InMemoryMessageQueue> getFunctionQueue(
      const faabric::Message& msg);

    void notifyCallFinished(const faabric::Message& msg);

    void notifyNodeFinished(const faabric::Message& msg);

    std::shared_ptr<InMemoryMessageQueue> getBindQueue();

    std::string getFunctionWarmSetName(const faabric::Message& msg);

    std::string getFunctionWarmSetNameFromStr(const std::string& funcStr);

    long getFunctionWarmNodeCount(const faabric::Message& msg);

    long getTotalWarmNodeCount();

    double getFunctionInFlightRatio(const faabric::Message& msg);

    long getFunctionInFlightCount(const faabric::Message& msg);

    void addHostToWarmSet(const std::string& funcStr);

    void removeHostFromWarmSet(const std::string& funcStr);

    void setTestMode(bool val);

    std::vector<unsigned int> getRecordedMessagesAll();

    std::vector<unsigned int> getRecordedMessagesLocal();

    std::vector<std::pair<std::string, unsigned int>>
    getRecordedMessagesShared();

    bool hasHostCapacity();

    std::string getThisHost();

    void broadcastFlush();

    void flushLocally();

    std::string getMessageStatus(unsigned int messageId);

    void setFunctionResult(faabric::Message& msg);

    faabric::Message getFunctionResult(unsigned int messageId, int timeout);

    void logChainedFunction(unsigned int parentMessageId,
                            unsigned int chainedMessageId);

    std::unordered_set<unsigned int> getChainedFunctions(unsigned int msgId);

    ExecGraph getFunctionExecGraph(unsigned int msgId);

  private:
    std::string thisHost;

    faabric::util::SystemConfig& conf;

    std::shared_ptr<InMemoryMessageQueue> bindQueue;

    std::shared_mutex mx;

    std::unordered_map<std::string, std::shared_ptr<InMemoryMessageQueue>>
      queueMap;
    std::unordered_map<std::string, long> nodeCountMap;
    std::unordered_map<std::string, long> inFlightCountMap;
    std::unordered_map<std::string, SchedulerOpinion> opinionMap;
    bool _hasHostCapacity = true;

    Resources thisHostResources;
    std::unordered_map<std::string, std::set<std::string>> registeredHosts;

    bool isTestMode = false;
    std::vector<unsigned int> recordedMessagesAll;
    std::vector<unsigned int> recordedMessagesLocal;
    std::vector<std::pair<std::string, unsigned int>> recordedMessagesShared;

    void updateOpinion(const faabric::Message& msg);

    void incrementInFlightCount(const faabric::Message& msg);

    void decrementInFlightCount(const faabric::Message& msg);

    void incrementWarmNodeCount(const faabric::Message& msg);

    void decrementWarmNodeCount(const faabric::Message& msg);

    int getFunctionMaxInFlightRatio(const faabric::Message& msg);

    ExecGraphNode getFunctionExecGraphNode(unsigned int msgId);

    int scheduleFunctionsOnHost(const std::string& host,
                                faabric::BatchExecuteRequest& req,
                                int offset);
};

Scheduler& getScheduler();
}
