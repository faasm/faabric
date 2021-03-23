#pragma once

#include <faabric/scheduler/ExecGraph.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>

#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/queue.h>

#include <shared_mutex>

#define AVAILABLE_HOST_SET "available_nodes"

namespace faabric::scheduler {

class Scheduler
{
  public:
    Scheduler();

    // ----------------------------------
    // External API
    // ----------------------------------
    void callFunction(faabric::Message& msg, bool forceLocal = false);

    std::vector<std::string> callFunctions(faabric::BatchExecuteRequest& req,
                                           bool forceLocal = false);

    void reset();

    void shutdown();

    void setTestMode(bool val);

    std::vector<unsigned int> getRecordedMessagesAll();

    std::vector<unsigned int> getRecordedMessagesLocal();

    // ----------------------------------
    // Internal API
    // ----------------------------------
    faabric::HostResources getThisHostResources();

    faabric::HostResources getHostResources(const std::string& host);

    void removeRegisteredHost(const std::string& host,
                              const faabric::Message& msg);

    void addHostToGlobalSet(const std::string& host);

    void addHostToGlobalSet();

    void removeHostFromGlobalSet();

    // ----------------------------------
    // Legacy
    // ----------------------------------
    std::shared_ptr<InMemoryMessageQueue> getFunctionQueue(
      const faabric::Message& msg);

    void notifyCallFinished(const faabric::Message& msg);

    void notifyFaasletFinished(const faabric::Message& msg);

    std::shared_ptr<InMemoryMessageQueue> getBindQueue();

    double getFunctionInFlightRatio(const faabric::Message& msg);

    long getFunctionInFlightCount(const faabric::Message& msg);

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
    std::unordered_map<std::string, long> faasletCounts;
    std::unordered_map<std::string, long> inFlightCounts;
    bool _hasHostCapacity = true;

    faabric::HostResources thisHostResources;
    std::unordered_map<std::string, std::set<std::string>> registeredHosts;

    bool isTestMode = false;
    std::vector<unsigned int> recordedMessagesAll;
    std::vector<unsigned int> recordedMessagesLocal;
    std::vector<std::pair<std::string, unsigned int>> recordedMessagesShared;

    void updateOpinion(const faabric::Message& msg);

    void incrementInFlightCount(const faabric::Message& msg);

    ExecGraphNode getFunctionExecGraphNode(unsigned int msgId);

    int scheduleFunctionsOnHost(const std::string& host,
                                faabric::BatchExecuteRequest& req,
                                std::vector<std::string>& records,
                                int offset);
};

Scheduler& getScheduler();
}
