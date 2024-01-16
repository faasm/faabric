#pragma once

#include <faabric/executor/ExecutorTask.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/clock.h>
#include <faabric/util/exception.h>
#include <faabric/util/queue.h>
#include <faabric/util/snapshot.h>

namespace faabric::executor {

class ChainedCallException : public faabric::util::FaabricException
{
  public:
    explicit ChainedCallException(std::string message)
      : FaabricException(std::move(message))
    {}
};

class Executor
{
  public:
    std::string id;

    explicit Executor(faabric::Message& msg);

    // Must be marked virtual to permit proper calling of subclass destructors
    virtual ~Executor();

    void executeTasks(std::vector<int> msgIdxs,
                      std::shared_ptr<faabric::BatchExecuteRequest> req);

    virtual void shutdown();

    virtual void reset(faabric::Message& msg);

    virtual int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    bool tryClaim();

    void claim();

    void releaseClaim();

    std::shared_ptr<faabric::util::SnapshotData> getMainThreadSnapshot(
      faabric::Message& msg,
      bool createIfNotExists = false);

    long getMillisSinceLastExec();

    virtual std::span<uint8_t> getMemoryView();

    virtual void restore(const std::string& snapshotKey);

    faabric::Message& getBoundMessage();

    bool isExecuting();

    bool isShutdown() { return _isShutdown; }

    void addChainedMessage(const faabric::Message& msg);

    const faabric::Message& getChainedMessage(int messageId);

    std::set<unsigned int> getChainedMessageIds();

    std::vector<faabric::util::SnapshotDiff> mergeDirtyRegions(
      const Message& msg,
      const std::vector<char>& extraDirtyPages = {});

    // FIXME: what is the right visibility?
    void setThreadResult(faabric::Message& msg,
                         int32_t returnValue,
                         const std::string& key,
                         const std::vector<faabric::util::SnapshotDiff>& diffs);

    virtual void setMemorySize(size_t newSize);

  protected:
    virtual size_t getMaxMemorySize();

    faabric::Message boundMessage;

    faabric::snapshot::SnapshotRegistry& reg;

    std::shared_ptr<faabric::util::DirtyTracker> tracker;

    uint32_t threadPoolSize = 0;

    std::map<int, std::shared_ptr<faabric::Message>> chainedMessages;

  private:
    // ---- Accounting ----
    std::atomic<bool> claimed = false;
    std::atomic<bool> _isShutdown = false;
    std::atomic<int> batchCounter = 0;
    std::atomic<int> threadBatchCounter = 0;
    faabric::util::TimePoint lastExec;

    // ---- Application threads ----
    std::shared_mutex threadExecutionMutex;
    std::vector<char> dirtyRegions;
    std::vector<std::vector<char>> threadLocalDirtyRegions;
    void deleteMainThreadSnapshot(const faabric::Message& msg);

    // ---- Function execution thread pool ----
    std::mutex threadsMutex;
    std::vector<std::shared_ptr<std::jthread>> threadPoolThreads;
    std::set<int> availablePoolThreads;

    std::vector<faabric::util::Queue<ExecutorTask>> threadTaskQueues;

    void threadPoolThread(std::stop_token st, int threadPoolIdx);
};
}
