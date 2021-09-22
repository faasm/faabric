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

        msg = faabric::util::messageFactory("foo", "bar");
    }

    ~DistributedSyncTestFixture()
    {
        faabric::util::setMockMode(false);
        sync.clear();
    }

  protected:
    DistributedSync& sync;
    faabric::Message msg;
};

TEST_CASE_METHOD(DistributedSyncTestFixture,
                 "Test remote requests sent on non-master",
                 "[sync]")
{
    std::string otherHost = "other";
    msg.set_masterhost(otherHost);

    faabric::FunctionGroupRequest::FunctionGroupOperation op =
      faabric::FunctionGroupRequest::FunctionGroupOperation::
        FunctionGroupRequest_FunctionGroupOperation_LOCK;

    SECTION("Lock")
    {
        op = faabric::FunctionGroupRequest::LOCK;
        sync.lock(msg);
    }

    std::vector<std::pair<std::string, faabric::FunctionGroupRequest>>
      actualRequests = getFunctionGroupRequests();

    REQUIRE(actualRequests.size() == 1);
    REQUIRE(actualRequests.at(0).first == otherHost);

    faabric::FunctionGroupRequest req = actualRequests.at(0).second;
    REQUIRE(req.operation() == op);
}

TEST_CASE_METHOD(DistributedSyncTestFixture,
                 "Test can't set group size on non-master",
                 "[sync]")
{
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
    bool failed = false;
    std::string errMsg;

    SECTION("Notify")
    {
        try {
            sync.localNotify(msg.appid());
        } catch (std::runtime_error& ex) {
            failed = true;
            errMsg = ex.what();
        }
    }

    SECTION("Barrier")
    {
        try {
            sync.localBarrier(msg.appid());
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

    sync.localLock(msg.appid());

    std::thread tA([this, &sharedInt] {
        getDistributedSync().localLock(msg.appid());

        assert(sharedInt == 99);
        sharedInt = 88;

        getDistributedSync().localUnlock(msg.appid());
    });

    // Main thread sleep for a while, make sure the other can't run and update
    // the counter
    SLEEP_MS(1000);

    REQUIRE(sharedInt == 0);
    sharedInt.store(99);

    sync.localUnlock(msg.appid());

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

    sync.setGroupSize(msg, nThreads);

    // Spawn n-1 child threads to add to shared sums over several barriers so
    // that the main thread can check all threads have completed after each.
    // We want to do this as many times as possible to deliberately create
    // contention

    int nSums = 1000;
    std::vector<std::atomic<int>> sharedSums(nSums);
    std::vector<std::thread> threads;
    for (int i = 1; i < nThreads; i++) {
        threads.emplace_back([this, nSums, &sharedSums] {
            for (int s = 0; s < nSums; s++) {
                sharedSums.at(s).fetch_add(s + 1);
                getDistributedSync().localBarrier(msg.appid());
            }
        });
    }

    for (int i = 0; i < nSums; i++) {
        sync.localBarrier(msg.appid());
        REQUIRE(sharedSums.at(i).load() == (i + 1) * (nThreads - 1));
    }

    // Join all child threads
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

TEST_CASE_METHOD(DistributedSyncTestFixture, "Test local try lock", "[sync]")
{
    int otherId = 345;

    // Should work for un-acquired lock
    REQUIRE(sync.localTryLock(msg.appid()));

    // Should also work for another lock
    REQUIRE(sync.localTryLock(otherId));

    // Should not work for already-acquired locks
    REQUIRE(!sync.localTryLock(msg.appid()));
    REQUIRE(!sync.localTryLock(otherId));

    // Should work again after unlock
    sync.localUnlock(msg.appid());

    REQUIRE(sync.localTryLock(msg.appid()));
    REQUIRE(!sync.localTryLock(otherId));

    sync.localUnlock(msg.appid());
    sync.localUnlock(otherId);

    REQUIRE(sync.localTryLock(msg.appid()));
    REQUIRE(sync.localTryLock(otherId));

    sync.localUnlock(msg.appid());
    sync.localUnlock(otherId);
}

TEST_CASE_METHOD(DistributedSyncTestFixture,
                 "Test local recursive lock",
                 "[sync]")
{
    std::atomic<int> sharedInt = 3;

    // Lock several times
    sync.localLockRecursive(msg.appid());
    sync.localLockRecursive(msg.appid());
    sync.localLockRecursive(msg.appid());

    // Unlock once
    sync.localUnlockRecursive(msg.appid());

    // Check background thread can't lock
    std::thread t([this, &sharedInt] {
        UNUSED(sharedInt);

        assert(sharedInt.load() == 3);

        // Won't be able to lock until main thread has unlocked
        sync.localLockRecursive(msg.appid());

        // Set to some other value
        sharedInt.store(4);

        sync.localUnlockRecursive(msg.appid());
    });

    // Allow other thread to start and block on locking
    SLEEP_MS(1000);
    REQUIRE(sharedInt == 3);

    // Unlock
    sync.localUnlockRecursive(msg.appid());
    sync.localUnlockRecursive(msg.appid());

    // Wait for other thread to finish
    if (t.joinable()) {
        t.join();
    }

    REQUIRE(sharedInt == 4);
}

TEST_CASE_METHOD(DistributedSyncTestFixture, "Test notify and await", "[sync]")
{
    int nThreads = 3;
    int actual[3] = { 0, 0, 0 };

    // Initialise the group size (including master thread)
    sync.setGroupSize(msg, nThreads + 1);

    std::vector<std::thread> threads;
    for (int i = 0; i < nThreads; i++) {
        threads.emplace_back([this, i, &actual] {
            // Make anything waiting wait
            SLEEP_MS(1000);
            actual[i] = i;

            sync.localNotify(msg.appid());
        });
    }

    // Master thread to await, should only go through once all threads have
    // finished
    sync.awaitNotify(msg.appid());

    for (int i = 0; i < nThreads; i++) {
        REQUIRE(actual[i] == i);
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}
}
