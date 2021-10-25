#include <catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/macros.h>
#include <faabric/util/testing.h>

using namespace faabric::transport;

#define CAPTURE_ERR_MSG(msgVar, op)                                            \
    try {                                                                      \
        op;                                                                    \
    } catch (std::runtime_error & ex) {                                        \
        errMsg = ex.what();                                                    \
    }

namespace tests {

class PointToPointGroupFixture
  : public ConfTestFixture
  , public PointToPointClientServerFixture
{
  public:
    PointToPointGroupFixture()
      : thisHost(conf.endpointHost)
    {
        faabric::util::setMockMode(true);

        setUpGroup(4);
    }

    ~PointToPointGroupFixture()
    {
        faabric::scheduler::clearMockRequests();
        faabric::util::setMockMode(false);
    }

    void setUpGroup(int groupSize)
    {
        msg = faabric::util::messageFactory("foo", "bar");
        msg.set_groupid(123);
        msg.set_groupsize(groupSize);

        // TODO - set up group

        group = broker.getGroup(msg.groupid());
    }

  protected:
    std::string thisHost;
    std::shared_ptr<PointToPointGroup> group = nullptr;

    faabric::Message msg;
};

TEST_CASE_METHOD(PointToPointGroupFixture,
                 "Test remote lock requests",
                 "[sync]")
{
    std::string otherHost = "other";
    group->overrideMasterHost(otherHost);

    int groupIdx = 2;
    msg.set_appindex(groupIdx);

    faabric::CoordinationRequest::CoordinationOperation op =
      faabric::CoordinationRequest::CoordinationOperation::
        CoordinationRequest_CoordinationOperation_LOCK;

    // Prepare the ptp message response
    // TODO - set up group

    std::vector<uint8_t> data(1, 0);

    bool recursive = false;

    SECTION("Lock")
    {
        op = faabric::CoordinationRequest::LOCK;

        broker.sendMessage(
          msg.groupid(), 0, groupIdx, data.data(), data.size());

        group->lock(groupIdx, false);
    }

    SECTION("Lock recursive")
    {
        op = faabric::CoordinationRequest::LOCK;

        broker.sendMessage(
          msg.groupid(), 0, groupIdx, data.data(), data.size());

        recursive = true;
        group->lock(groupIdx, recursive);
    }

    SECTION("Unlock")
    {
        op = faabric::CoordinationRequest::UNLOCK;
        group->unlock(groupIdx, false);
    }

    SECTION("Unlock recursive")
    {
        op = faabric::CoordinationRequest::UNLOCK;
        recursive = true;
        group->unlock(groupIdx, recursive);
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

TEST_CASE_METHOD(PointToPointGroupFixture,
                 "Test local locking and unlocking",
                 "[sync]")
{
    std::atomic<int> sharedInt = 0;

    group->localLock();

    std::thread tA([this, &sharedInt] {
        group->localLock();

        assert(sharedInt == 99);
        sharedInt = 88;

        group->localUnlock();
    });

    // Main thread sleep for a while, make sure the other can't run and update
    // the counter
    SLEEP_MS(1000);

    REQUIRE(sharedInt == 0);
    sharedInt.store(99);

    group->localUnlock();

    if (tA.joinable()) {
        tA.join();
    }

    REQUIRE(sharedInt == 88);
}

TEST_CASE_METHOD(PointToPointGroupFixture,
                 "Test distributed coordination barrier",
                 "[sync]")
{
    int nThreads = 5;
    setUpGroup(nThreads);

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
                group->barrier(i);
            }
        });
    }

    for (int i = 0; i < nSums; i++) {
        group->barrier(0);
        REQUIRE(sharedSums.at(i).load() == (i + 1) * (nThreads - 1));
    }

    // Join all child threads
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

TEST_CASE_METHOD(PointToPointGroupFixture,
                 "Test local try lock",
                 "[sync]")
{
    faabric::Message otherMsg = faabric::util::messageFactory("foo", "other");
    otherMsg.set_groupid(345);
    otherMsg.set_groupsize(2);

    // TODO - set up group
    auto otherGroup = broker.getGroup(otherMsg.groupid());

    // Should work for un-acquired lock
    REQUIRE(group->localTryLock());

    // Should also work for another lock
    REQUIRE(otherGroup->localTryLock());

    // Should not work for already-acquired locks
    REQUIRE(!group->localTryLock());
    REQUIRE(!otherGroup->localTryLock());

    // Should work again after unlock
    group->localUnlock();

    REQUIRE(group->localTryLock());
    REQUIRE(!otherGroup->localTryLock());

    // Running again should have no effect
    group->localUnlock();

    // Unlock other group
    otherGroup->localUnlock();

    REQUIRE(group->localTryLock());
    REQUIRE(otherGroup->localTryLock());

    group->localUnlock();
    otherGroup->localUnlock();
}

TEST_CASE_METHOD(PointToPointGroupFixture,
                 "Test notify and await",
                 "[sync]")
{
    int nThreads = 4;
    int actual[4] = { 0, 0, 0, 0 };

    // TODO - set up group

    // Run threads in background to force a wait from the master
    std::vector<std::thread> threads;
    for (int i = 1; i < nThreads; i++) {
        threads.emplace_back([this, i, &actual] {
            SLEEP_MS(1000);
            actual[i] = i;

            group->notify(i);
        });
    }

    // Master thread to await, should only go through once all threads have
    // finished
    group->notify(0);

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
