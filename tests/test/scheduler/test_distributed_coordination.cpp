#include <catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/DistributedCoordinator.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/macros.h>
#include <faabric/util/testing.h>

using namespace faabric::scheduler;

#define CAPTURE_ERR_MSG(msgVar, op)                                            \
    try {                                                                      \
        op;                                                                    \
    } catch (std::runtime_error & ex) {                                        \
        errMsg = ex.what();                                                    \
    }

namespace tests {

class DistributedCoordinatorTestFixture : public ConfTestFixture
{
  public:
    DistributedCoordinatorTestFixture()
      : sync(getDistributedCoordinator())
    {
        faabric::util::setMockMode(true);

        sync.clear();

        msg = faabric::util::messageFactory("foo", "bar");

        msg.set_groupid(123);
        msg.set_groupsize(10);
    }

    ~DistributedCoordinatorTestFixture()
    {
        faabric::scheduler::clearMockRequests();
        faabric::util::setMockMode(false);
        sync.clear();
    }

  protected:
    DistributedCoordinator& sync;
    faabric::Message msg;
};

TEST_CASE_METHOD(DistributedCoordinatorTestFixture,
                 "Test remote requests sent on non-master",
                 "[sync]")
{
    std::string otherHost = "other";
    msg.set_masterhost(otherHost);

    faabric::CoordinationRequest::CoordinationOperation op =
      faabric::CoordinationRequest::CoordinationOperation::
        CoordinationRequest_CoordinationOperation_LOCK;

    SECTION("Lock")
    {
        op = faabric::CoordinationRequest::LOCK;
        sync.lock(msg);
    }

    SECTION("Unlock")
    {
        op = faabric::CoordinationRequest::UNLOCK;
        sync.unlock(msg);
    }

    SECTION("Barrier")
    {
        op = faabric::CoordinationRequest::BARRIER;
        sync.barrier(msg);
    }

    SECTION("Notify")
    {
        op = faabric::CoordinationRequest::NOTIFY;
        sync.notify(msg);
    }

    std::vector<std::pair<std::string, faabric::CoordinationRequest>>
      actualRequests = getCoordinationRequests();

    REQUIRE(actualRequests.size() == 1);
    REQUIRE(actualRequests.at(0).first == otherHost);

    faabric::CoordinationRequest req = actualRequests.at(0).second;
    REQUIRE(req.operation() == op);
}

TEST_CASE_METHOD(DistributedCoordinatorTestFixture,
                 "Test operations fail when group size and id not set",
                 "[sync]")
{
    std::string errMsg;
    std::string expectedErrMsg;

    std::string noGroupIdMsg = "Message does not have group id set";
    std::string noGroupSizeMsg = "Message does not have group size set";

    SECTION("Lock")
    {
        msg.set_groupid(0);
        expectedErrMsg = noGroupIdMsg;
        CAPTURE_ERR_MSG(errMsg, sync.localLock(msg));
    }

    SECTION("Unlock")
    {
        msg.set_groupid(0);
        expectedErrMsg = noGroupIdMsg;
        CAPTURE_ERR_MSG(errMsg, sync.localLock(msg));
    }

    SECTION("Notify")
    {
        SECTION("Without group size")
        {
            msg.set_groupsize(0);
            expectedErrMsg = noGroupSizeMsg;
        }

        SECTION("Without group id")
        {
            msg.set_groupid(0);
            expectedErrMsg = noGroupIdMsg;
        }

        CAPTURE_ERR_MSG(errMsg, sync.localNotify(msg));
    }

    SECTION("Barrier without group size")
    {
        SECTION("Without group size")
        {
            msg.set_groupsize(0);
            expectedErrMsg = noGroupSizeMsg;
        }

        SECTION("Without group id")
        {
            msg.set_groupid(0);
            expectedErrMsg = noGroupIdMsg;
        }

        CAPTURE_ERR_MSG(errMsg, sync.localBarrier(msg));
    }

    REQUIRE(errMsg == expectedErrMsg);
}

TEST_CASE_METHOD(DistributedCoordinatorTestFixture,
                 "Test local locking and unlocking",
                 "[sync]")
{
    std::atomic<int> sharedInt = 0;

    sync.localLock(msg);

    std::thread tA([this, &sharedInt] {
        getDistributedCoordinator().localLock(msg);

        assert(sharedInt == 99);
        sharedInt = 88;

        getDistributedCoordinator().localUnlock(msg);
    });

    // Main thread sleep for a while, make sure the other can't run and update
    // the counter
    SLEEP_MS(1000);

    REQUIRE(sharedInt == 0);
    sharedInt.store(99);

    sync.localUnlock(msg);

    if (tA.joinable()) {
        tA.join();
    }

    REQUIRE(sharedInt == 88);
}

TEST_CASE_METHOD(DistributedCoordinatorTestFixture,
                 "Test sync barrier locally",
                 "[sync]")
{
    int nThreads = 5;
    msg.set_groupsize(nThreads);

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
                getDistributedCoordinator().localBarrier(msg);
            }
        });
    }

    for (int i = 0; i < nSums; i++) {
        sync.localBarrier(msg);
        REQUIRE(sharedSums.at(i).load() == (i + 1) * (nThreads - 1));
    }

    // Join all child threads
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

TEST_CASE_METHOD(DistributedCoordinatorTestFixture,
                 "Test local try lock",
                 "[sync]")
{
    faabric::Message otherMsg = faabric::util::messageFactory("foo", "other");
    otherMsg.set_groupid(345);

    // Should work for un-acquired lock
    REQUIRE(sync.localTryLock(msg));

    // Should also work for another lock
    REQUIRE(sync.localTryLock(otherMsg));

    // Should not work for already-acquired locks
    REQUIRE(!sync.localTryLock(msg));
    REQUIRE(!sync.localTryLock(otherMsg));

    // Should work again after unlock
    sync.localUnlock(msg);

    REQUIRE(sync.localTryLock(msg));
    REQUIRE(!sync.localTryLock(otherMsg));

    sync.localUnlock(msg);
    sync.localUnlock(otherMsg);

    REQUIRE(sync.localTryLock(msg));
    REQUIRE(sync.localTryLock(otherMsg));

    sync.localUnlock(msg);
    sync.localUnlock(otherMsg);
}

TEST_CASE_METHOD(DistributedCoordinatorTestFixture,
                 "Test local recursive lock",
                 "[sync]")
{
    std::atomic<int> sharedInt = 3;

    // Lock several times
    sync.localLockRecursive(msg);
    sync.localLockRecursive(msg);
    sync.localLockRecursive(msg);

    // Unlock once
    sync.localUnlockRecursive(msg);

    // Check background thread can't lock
    std::thread t([this, &sharedInt] {
        UNUSED(sharedInt);

        assert(sharedInt.load() == 3);

        // Won't be able to lock until main thread has unlocked
        sync.localLockRecursive(msg);

        // Set to some other value
        sharedInt.store(4);

        sync.localUnlockRecursive(msg);
    });

    // Allow other thread to start and block on locking
    SLEEP_MS(1000);
    REQUIRE(sharedInt == 3);

    // Unlock
    sync.localUnlockRecursive(msg);
    sync.localUnlockRecursive(msg);

    // Wait for other thread to finish
    if (t.joinable()) {
        t.join();
    }

    REQUIRE(sharedInt == 4);
}

TEST_CASE_METHOD(DistributedCoordinatorTestFixture,
                 "Test notify and await",
                 "[sync]")
{
    int nThreads = 3;
    int actual[3] = { 0, 0, 0 };

    // Initialise the group size (including master thread)
    msg.set_groupsize(nThreads + 1);

    std::vector<std::thread> threads;
    for (int i = 0; i < nThreads; i++) {
        threads.emplace_back([this, i, &actual] {
            // Make anything waiting wait
            SLEEP_MS(1000);
            actual[i] = i;

            sync.localNotify(msg);
        });
    }

    // Master thread to await, should only go through once all threads have
    // finished
    sync.awaitNotify(msg);

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
