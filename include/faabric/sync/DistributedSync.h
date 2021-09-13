#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/barrier.h>
#include <faabric/util/locks.h>

namespace faabric::sync {

class DistributedSync
{
  public:
    DistributedSync();

    void initGroup(const faabric::Message& msg, int groupSize);

    void clear();

    void localLock(int32_t groupId);

    void lock(const faabric::Message& msg);

    void localUnlock(int32_t groupId);

    void unlock(const faabric::Message& msg);

    void localNotify(int32_t groupId);

    void localNotifyMaster(int32_t groupId);

    void notify(const faabric::Message& msg);

    void localBarrier(int32_t groupId);

    void barrier(const faabric::Message& msg);

  private:
    faabric::scheduler::Scheduler& sch;

    std::shared_mutex sharedMutex;

    std::unordered_map<int32_t, int32_t> localGroups;

    std::unordered_map<uint32_t, std::shared_ptr<faabric::util::Barrier>>
      barriers;

    std::unordered_map<uint32_t, std::shared_ptr<std::recursive_mutex>>
      recursiveMutexes;

    std::unordered_map<uint32_t, std::shared_ptr<std::mutex>> mutexes;
    std::unordered_map<uint32_t, std::shared_ptr<std::atomic<int>>> counts;
    std::unordered_map<uint32_t, std::shared_ptr<std::condition_variable>> cvs;

    void doLocalNotify(int32_t groupId, bool master);

    void checkGroupExistsLocally(int32_t groupId);
};

DistributedSync& getDistributedSync();
}

