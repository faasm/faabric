#pragma once

#include "faabric/proto/faabric.pb.h"
#include <faabric/scheduler/ExecGraph.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>

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

    std::vector<std::string> callFunctions(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
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

    void forceEnqueueMessage(const faabric::Message& msg);

    faabric::Message getNextMessageForFunction(const faabric::Message& msg,
                                               int timeout);

    std::shared_ptr<InMemoryBatchQueue> getFunctionQueue(
      const faabric::Message& msg);

    std::shared_ptr<InMemoryBatchQueue> getFunctionQueue(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

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

  private:
    std::string thisHost;

    faabric::util::SystemConfig& conf;

    std::shared_ptr<InMemoryMessageQueue> bindQueue;

    std::shared_mutex mx;

    InMemoryBatchQueueMap queueMap;
    std::unordered_map<std::string, long> faasletCounts;
    std::unordered_map<std::string, long> inFlightCounts;

    faabric::HostResources thisHostResources;
    std::unordered_map<std::string, std::unordered_set<std::string>>
      registeredHosts;

    std::vector<unsigned int> recordedMessagesAll;
    std::vector<unsigned int> recordedMessagesLocal;
    std::vector<std::pair<std::string, unsigned int>> recordedMessagesShared;

    faabric::HostResources getHostResources(const std::string& host);

    void incrementInFlightCount(const faabric::Message& msg, int count);

    void addFaaslets(const faabric::Message& msg);

    void addFaasletsForBatch(const faabric::Message& msg);

    void doAddFaaslets(const faabric::Message& msg, int count);

    ExecGraphNode getFunctionExecGraphNode(unsigned int msgId);

    int scheduleFunctionsOnHost(
      const std::string& host,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      std::vector<std::string>& records,
      int offset);
};

Scheduler& getScheduler();
}
