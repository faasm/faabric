#include "faabric_utils.h"
#include "fixtures.h"
#include <catch.hpp>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/DistributedSync.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/macros.h>
#include <faabric/util/testing.h>

using namespace faabric::scheduler;

namespace tests {

class DistributedSyncTestFixture : public ConfTestFixture
{
  public:
    DistributedSyncTestFixture()
      : sync(getDistributedSync())
    {
        faabric::util::setMockMode(true);
    }

    ~DistributedSyncTestFixture()
    {
        faabric::util::setMockMode(false);
        sync.clear();
    }

  protected:
    DistributedSync& sync;
};

TEST_CASE_METHOD(DistributedSyncTestFixture,
                 "Test can't set group size on non-master",
                 "[sync]")
{
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    msg.set_masterhost("blahhost");

    bool failed = false;
    std::string actualMsg;

    try {
        sync.setGroupSize(msg, 123);
    } catch (std::runtime_error& ex) {
        failed = true;
        actualMsg = ex.what();
    }

    REQUIRE(failed);
    REQUIRE(actualMsg == "Setting sync group size on non-master");
}

TEST_CASE_METHOD(DistributedSyncTestFixture,
                 "Test operations fail when group size not set",
                 "[sync]")
{
    DistributedSync& sync = getDistributedSync();
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    int groupId = msg.appid();

    bool failed = false;
    std::string errMsg;

    SECTION("Notify")
    {
        try {
            sync.localNotify(groupId);
        } catch (std::runtime_error& ex) {
            failed = true;
            errMsg = ex.what();
        }
    }

    SECTION("Barrier")
    {
        try {
            sync.localBarrier(groupId);
        } catch (std::runtime_error& ex) {
            failed = true;
            errMsg = ex.what();
        }
    }

    REQUIRE(failed);
    REQUIRE(errMsg == "Group size not set");
}

TEST_CASE_METHOD(DistributedSyncTestFixture,
                 "Test local locking and unlocking",
                 "[sync]")
{
    std::atomic<int> sharedInt = 0;
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    int groupId = msg.appid();

    DistributedSync& sync = getDistributedSync();
    sync.localLock(groupId);

    std::thread tA([&sharedInt, groupId] {
        getDistributedSync().localLock(groupId);

        assert(sharedInt == 99);
        sharedInt = 88;

        getDistributedSync().localUnlock(groupId);
    });

    // Main thread sleep for a while, make sure the other can't run and update
    // the counter
    usleep(1000 * 1000);

    REQUIRE(sharedInt == 0);
    sharedInt.store(99);

    sync.localUnlock(groupId);

    if (tA.joinable()) {
        tA.join();
    }

    REQUIRE(sharedInt == 88);
}

TEST_CASE_METHOD(DistributedSyncTestFixture,
                 "Test sync barrier locally",
                 "[sync]")
{
    int nThreads = 5;

    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    int groupId = msg.appid();

    DistributedSync& sync = getDistributedSync();
    sync.setGroupSize(msg, nThreads);

    // Spawn n-1 child threads to add to shared sums over several barriers so
    // that the main thread can check all threads have completed after each.
    // We want to do this as many times as possible to deliberately create
    // contention

    int nSums = 1000;
    std::vector<std::atomic<int>> sharedSums(nSums);
    std::vector<std::thread> threads;
    for (int i = 1; i < nThreads; i++) {
        threads.emplace_back([groupId, nSums, &sharedSums] {
            for (int s = 0; s < nSums; s++) {
                sharedSums.at(s).fetch_add(s + 1);
                getDistributedSync().localBarrier(groupId);
            }
        });
    }

    for (int i = 0; i < nSums; i++) {
        sync.localBarrier(groupId);
        REQUIRE(sharedSums.at(i).load() == (i + 1) * (nThreads - 1));
    }

    // Join all child threads
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}
}
