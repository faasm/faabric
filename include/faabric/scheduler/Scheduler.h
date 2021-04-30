#pragma once

#include <faabric/scheduler/ExecGraph.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>
#include <faabric/transport/MessageContext.h>

#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/queue.h>

#include <shared_mutex>

#define AVAILABLE_HOST_SET "available_hosts"

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

    void broadcastSnapshotDelete(const faabric::Message& msg,
                                 const std::string& snapshotKey);

    void reset();

    void shutdown();

    void notifyCallFinished(const faabric::Message& msg);

    void notifyFaasletFinished(const faabric::Message& msg);

    long getFunctionInFlightCount(const faabric::Message& msg);

    long getFunctionFaasletCount(const faabric::Message& msg);

    int getFunctionRegisteredHostCount(const faabric::Message& msg);

    std::unordered_set<std::string> getFunctionRegisteredHosts(
      const faabric::Message& msg);

    void broadcastFlush();

    void flushLocally();

    std::string getMessageStatus(unsigned int messageId);

    void setFunctionResult(faabric::Message& msg);

    faabric::Message getFunctionResult(unsigned int messageId, int timeout);

    std::string getThisHost();

    std::shared_ptr<InMemoryMessageQueue> getFunctionQueue(
      const faabric::Message& msg);

    std::shared_ptr<InMemoryMessageQueue> getBindQueue();

    std::unordered_set<std::string> getAvailableHosts();

    void addHostToGlobalSet();

    void addHostToGlobalSet(const std::string& host);

    void removeHostFromGlobalSet(const std::string& host);

    void removeRegisteredHost(const std::string& host,
                              const faabric::Message& msg);

    faabric::HostResources getThisHostResources();

    void setThisHostResources(faabric::HostResources& res);

    // ----------------------------------
    // Testing
    // ----------------------------------
    std::vector<unsigned int> getRecordedMessagesAll();

    std::vector<unsigned int> getRecordedMessagesLocal();

    std::vector<std::pair<std::string, unsigned int>>
    getRecordedMessagesShared();

    // ----------------------------------
    // Exec graph
    // ----------------------------------
    void logChainedFunction(unsigned int parentMessageId,
                            unsigned int chainedMessageId);

    std::unordered_set<unsigned int> getChainedFunctions(unsigned int msgId);

    ExecGraph getFunctionExecGraph(unsigned int msgId);

    // ----------------------------------
    // Message transport
    // ----------------------------------
    faabric::transport::MessageContext messageContext;

  private:
    std::string thisHost;

    faabric::util::SystemConfig& conf;

    std::shared_ptr<InMemoryMessageQueue> bindQueue;

    std::shared_mutex mx;

    std::unordered_map<std::string, std::shared_ptr<InMemoryMessageQueue>>
      queueMap;
    std::unordered_map<std::string, long> faasletCounts;
    std::unordered_map<std::string, long> inFlightCounts;

    faabric::HostResources thisHostResources;
    std::unordered_map<std::string, std::unordered_set<std::string>>
      registeredHosts;

    std::vector<unsigned int> recordedMessagesAll;
    std::vector<unsigned int> recordedMessagesLocal;
    std::vector<std::pair<std::string, unsigned int>> recordedMessagesShared;

    faabric::HostResources getHostResources(const std::string& host);

    void incrementInFlightCount(const faabric::Message& msg);

    void addFaaslets(const faabric::Message& msg);

    ExecGraphNode getFunctionExecGraphNode(unsigned int msgId);

    int scheduleFunctionsOnHost(const std::string& host,
                                faabric::BatchExecuteRequest& req,
                                std::vector<std::string>& records,
                                int offset);
};

Scheduler& getScheduler();
}
