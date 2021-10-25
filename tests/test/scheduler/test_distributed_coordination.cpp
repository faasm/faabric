#include <catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/DistributedCoordinator.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointBroker.h>
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

class DistributedCoordinationGroupFixture
  : public ConfTestFixture
  , public PointToPointClientServerFixture
  , public DistributedCoordinationTestFixture
{
  public:
    DistributedCoordinationGroupFixture()
      : thisHost(conf.endpointHost)
    {
        faabric::util::setMockMode(true);

        setUpGroup(4);
    }

    ~DistributedCoordinationGroupFixture()
    {
        faabric::scheduler::clearMockRequests();
        faabric::util::setMockMode(false);
        distCoord.clear();
    }

    void setUpGroup(int groupSize)
    {
        distCoord.clear();

        msg = faabric::util::messageFactory("foo", "bar");
        msg.set_groupid(123);
        msg.set_groupsize(groupSize);

        distCoord.initGroup(msg);

        coordGroup = distCoord.getCoordinationGroup(msg.groupid());
    }

  protected:
    std::string thisHost;
    std::shared_ptr<DistributedCoordinationGroup> coordGroup = nullptr;

    faabric::Message msg;
};

TEST_CASE_METHOD(DistributedCoordinationGroupFixture,
                 "Test remote lock requests",
                 "[sync]")
{
    std::string otherHost = "other";
    coordGroup->overrideMasterHost(otherHost);

    int groupIdx = 2;
    msg.set_appindex(groupIdx);

    faabric::CoordinationRequest::CoordinationOperation op =
      faabric::CoordinationRequest::CoordinationOperation::
        CoordinationRequest_CoordinationOperation_LOCK;

    // Prepare the ptp message response
    broker.setHostForReceiver(msg.groupid(), groupIdx, thisHost);
    std::vector<uint8_t> data(1, 0);

    bool recursive = false;

    SECTION("Lock")
    {
        op = faabric::CoordinationRequest::LOCK;

        broker.sendMessage(
          msg.groupid(), 0, groupIdx, data.data(), data.size());

        coordGroup->lock(groupIdx, false);
    }

    SECTION("Lock recursive")
    {
        op = faabric::CoordinationRequest::LOCK;

        broker.sendMessage(
          msg.groupid(), 0, groupIdx, data.data(), data.size());

        recursive = true;
        coordGroup->lock(groupIdx, recursive);
    }

    SECTION("Unlock")
    {
        op = faabric::CoordinationRequest::UNLOCK;
        coordGroup->unlock(groupIdx, false);
    }

    SECTION("Unlock recursive")
    {
        op = faabric::CoordinationRequest::UNLOCK;
        recursive = true;
        coordGroup->unlock(groupIdx, recursive);
    }

    std::vector<std::pair<std::string, faabric::CoordinationRequest>>
      actualRequests = getCoordinationRequests();

    REQUIRE(actualRequests.size() == 1);
    REQUIRE(actualRequests.at(0).first == thisHost);

    faabric::CoordinationRequest req = actualRequests.at(0).second;
    REQUIRE(req.operation() == op);
    REQUIRE(req.fromhost() == thisHost);
    REQUIRE(req.recursive() == recursive);
}

TEST_CASE_METHOD(DistributedCoordinationGroupFixture,
                 "Test local locking and unlocking",
                 "[sync]")
{
    std::atomic<int> sharedInt = 0;

    coordGroup->localLock();

    std::thread tA([this, &sharedInt] {
        coordGroup->localLock();

        assert(sharedInt == 99);
        sharedInt = 88;

        coordGroup->localUnlock();
    });

    // Main thread sleep for a while, make sure the other can't run and update
    // the counter
    SLEEP_MS(1000);

    REQUIRE(sharedInt == 0);
    sharedInt.store(99);

    coordGroup->localUnlock();

    if (tA.joinable()) {
        tA.join();
    }

    REQUIRE(sharedInt == 88);
}

TEST_CASE_METHOD(DistributedCoordinationGroupFixture,
                 "Test distributed coordination barrier",
                 "[sync]")
{
    int nThreads = 5;
    setUpGroup(nThreads);

    // Prepare point to point message mappings
    for (int i = 0; i < nThreads; i++) {
        broker.setHostForReceiver(msg.groupid(), i, thisHost);
    }

    int nSums = 2;
    SECTION("Single operation") { nSums = 1; }

    SECTION("Lots of operations")
    {
        // We want to do this as many times as possible to deliberately create
        // contention
        nSums = 1000;
    }

    // Spawn n-1 child threads to add to shared sums over several barriers so
    // that the main thread can check all threads have completed after each.
    std::vector<std::atomic<int>> sharedSums(nSums);
    std::vector<std::thread> threads;
    for (int i = 1; i < nThreads; i++) {
        threads.emplace_back([this, i, nSums, &sharedSums] {
            for (int s = 0; s < nSums; s++) {
                sharedSums.at(s).fetch_add(s + 1);
                coordGroup->barrier(i);
            }
        });
    }

    for (int i = 0; i < nSums; i++) {
        coordGroup->barrier(0);
        REQUIRE(sharedSums.at(i).load() == (i + 1) * (nThreads - 1));
    }

    // Join all child threads
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

TEST_CASE_METHOD(DistributedCoordinationGroupFixture,
                 "Test local try lock",
                 "[sync]")
{
    faabric::Message otherMsg = faabric::util::messageFactory("foo", "other");
    otherMsg.set_groupid(345);
    otherMsg.set_groupsize(2);

    distCoord.initGroup(otherMsg);

    auto otherCoordGroup = distCoord.getCoordinationGroup(otherMsg.groupid());

    // Should work for un-acquired lock
    REQUIRE(coordGroup->localTryLock());

    // Should also work for another lock
    REQUIRE(otherCoordGroup->localTryLock());

    // Should not work for already-acquired locks
    REQUIRE(!coordGroup->localTryLock());
    REQUIRE(!otherCoordGroup->localTryLock());

    // Should work again after unlock
    coordGroup->localUnlock();

    REQUIRE(coordGroup->localTryLock());
    REQUIRE(!otherCoordGroup->localTryLock());

    // Running again should have no effect
    coordGroup->localUnlock();

    // Unlock other group
    otherCoordGroup->localUnlock();

    REQUIRE(coordGroup->localTryLock());
    REQUIRE(otherCoordGroup->localTryLock());

    coordGroup->localUnlock();
    otherCoordGroup->localUnlock();
}

TEST_CASE_METHOD(DistributedCoordinationGroupFixture,
                 "Test notify and await",
                 "[sync]")
{
    int nThreads = 4;
    int actual[4] = { 0, 0, 0, 0 };

    // Initialise the group
    setUpGroup(nThreads);

    // Prepare point to point message mappings
    for (int i = 0; i < nThreads; i++) {
        broker.setHostForReceiver(msg.groupid(), i, thisHost);
    }

    // Run threads in background to force a wait from the master
    std::vector<std::thread> threads;
    for (int i = 1; i < nThreads; i++) {
        threads.emplace_back([this, i, &actual] {
            SLEEP_MS(1000);
            actual[i] = i;

            coordGroup->notify(i);
        });
    }

    // Master thread to await, should only go through once all threads have
    // finished
    coordGroup->notify(0);

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
