#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointClient.h>
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
  : public ConfFixture
  , public PointToPointClientServerFixture
  , public SchedulerFixture
{
  public:
    PointToPointGroupFixture()
      : thisHost(conf.endpointHost)
    {
        faabric::util::setMockMode(true);
    }

    ~PointToPointGroupFixture()
    {
        faabric::scheduler::clearMockRequests();
        faabric::util::setMockMode(false);
    }

    std::shared_ptr<PointToPointGroup> setUpGroup(int appId,
                                                  int groupId,
                                                  int groupSize)
    {
        req = faabric::util::batchExecFactory("foo", "bar", groupSize);

        faabric::batch_scheduler::SchedulingDecision decision(appId, groupId);

        for (int i = 0; i < groupSize; i++) {
            auto& msg = req->mutable_messages()->at(i);
            msg.set_appid(appId);
            msg.set_groupid(groupId);
            msg.set_appidx(i);
            msg.set_groupidx(i);

            decision.addMessage(thisHost, msg);
        }

        broker.setUpLocalMappingsFromSchedulingDecision(decision);

        return PointToPointGroup::getGroup(groupId);
    }

  protected:
    std::string thisHost;

    std::shared_ptr<faabric::BatchExecuteRequest> req = nullptr;
};

TEST_CASE_METHOD(PointToPointGroupFixture,
                 "Test removing mappings from point-to-point broker",
                 "[transport][ptp]")
{
    int appId = 123;
    int groupId = 456;
    int groupSize = 3;

    auto group = setUpGroup(appId, groupId, groupSize);

    REQUIRE(broker.getIdxsRegisteredForGroup(groupId).size() == groupSize);

    SECTION("Clear through broker") { broker.clear(); }

    SECTION("Clear through scheduler reset")
    {
        faabric::scheduler::getScheduler().reset();
    }

    REQUIRE(broker.getIdxsRegisteredForGroup(groupId).empty());
}

TEST_CASE_METHOD(PointToPointGroupFixture,
                 "Test lock requests",
                 "[ptp][transport]")
{
    std::string otherHost = "other";

    int appId = 123;
    int groupId = 345;
    int groupIdx = 1;

    faabric::batch_scheduler::SchedulingDecision decision(appId, groupId);

    faabric::Message msgA = faabric::util::messageFactory("foo", "bar");
    msgA.set_appid(appId);
    msgA.set_groupid(groupId);
    msgA.set_appidx(0);
    msgA.set_groupidx(0);
    decision.addMessage(otherHost, msgA);

    faabric::Message msgB = faabric::util::messageFactory("foo", "bar");
    msgB.set_appid(appId);
    msgB.set_groupid(groupId);
    msgB.set_appidx(groupIdx);
    msgB.set_groupidx(groupIdx);
    decision.addMessage(thisHost, msgB);

    broker.setUpLocalMappingsFromSchedulingDecision(decision);
    auto group = PointToPointGroup::getGroup(groupId);

    PointToPointCall op;

    std::vector<uint8_t> data(1, 0);

    bool recursive = false;

    SECTION("Lock")
    {
        op = PointToPointCall::LOCK_GROUP;

        // Prepare response
        broker.sendMessage(groupId,
                           POINT_TO_POINT_MASTER_IDX,
                           groupIdx,
                           data.data(),
                           data.size());

        group->lock(groupIdx, false);
    }

    SECTION("Lock recursive")
    {
        op = PointToPointCall::LOCK_GROUP_RECURSIVE;
        recursive = true;

        // Prepare response
        broker.sendMessage(groupId,
                           POINT_TO_POINT_MASTER_IDX,
                           groupIdx,
                           data.data(),
                           data.size());

        group->lock(groupIdx, recursive);
    }

    SECTION("Unlock")
    {
        op = PointToPointCall::UNLOCK_GROUP;
        group->unlock(groupIdx, false);
    }

    SECTION("Unlock recursive")
    {
        op = PointToPointCall::UNLOCK_GROUP_RECURSIVE;
        recursive = true;
        group->unlock(groupIdx, recursive);
    }

    std::vector<
      std::tuple<std::string, PointToPointCall, faabric::PointToPointMessage>>
      actualRequests = getSentLockMessages();

    REQUIRE(actualRequests.size() == 1);
    REQUIRE(std::get<0>(actualRequests.at(0)) == otherHost);

    PointToPointCall actualOp = std::get<1>(actualRequests.at(0));
    REQUIRE(actualOp == op);

    faabric::PointToPointMessage req = std::get<2>(actualRequests.at(0));
    REQUIRE(req.appid() == appId);
    REQUIRE(req.groupid() == groupId);
    REQUIRE(req.sendidx() == groupIdx);
    REQUIRE(req.recvidx() == POINT_TO_POINT_MASTER_IDX);
}

TEST_CASE_METHOD(PointToPointGroupFixture,
                 "Test locking and unlocking",
                 "[ptp][transport]")
{
    int appId = 123;
    int groupId = 234;

    int nThreads = 4;
    int nLoops = 50;

    auto group = setUpGroup(appId, groupId, nThreads);

    std::atomic<bool> success = true;

    int criticalVar = 1;

    bool useLocal = false;
    bool recursive = false;
    SECTION("Local-only") { useLocal = true; }

    SECTION("Distributed version non-recursive")
    {
        useLocal = false;
        recursive = false;
    }

    SECTION("Distributed version recursive")
    {
        useLocal = false;
        recursive = true;
    }

    // Create high contention on the critical var that will be detected if
    // locking isn't working.
    std::vector<std::jthread> threads;
    for (int i = 0; i < nThreads; i++) {
        threads.emplace_back(
          [useLocal, recursive, i, nLoops, &group, &criticalVar, &success] {
              if (useLocal) {
                  group->localLock();
              } else {
                  group->lock(i, recursive);
              }

              // Check that while in this critical section, no changes from
              // other threads are visible

              criticalVar = 2;
              for (int j = 0; j < nLoops; j++) {
                  // Set the var
                  criticalVar = i;

                  // Sleep a bit
                  int sleepTimeMs = std::rand() % 30;
                  SLEEP_MS(sleepTimeMs);

                  // Check the var is unchanged by others
                  if (criticalVar != i) {
                      SPDLOG_ERROR("Inner loop testing locking got {} != {}",
                                   criticalVar,
                                   i);
                      success = false;
                  }
              }

              if (useLocal) {
                  group->localUnlock();
              } else {
                  group->unlock(i, recursive);
              }
          });
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    REQUIRE(success);
}

TEST_CASE_METHOD(PointToPointGroupFixture,
                 "Test distributed coordination barrier",
                 "[ptp][transport]")
{
    int nThreads = 5;
    int appId = 123;
    int groupId = 555;

    auto group = setUpGroup(appId, groupId, nThreads);

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
    std::vector<std::jthread> threads;
    for (int i = 1; i < nThreads; i++) {
        threads.emplace_back([&group, i, nSums, &sharedSums] {
            for (int s = 0; s < nSums; s++) {
                sharedSums.at(s).fetch_add(s + 1);
                group->barrier(i);
            }
        });
    }

    for (int i = 0; i < nSums; i++) {
        group->barrier(POINT_TO_POINT_MASTER_IDX);
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
                 "[ptp][transport]")
{
    // Set up one group
    int nThreads = 5;
    int appId = 11;
    int groupId = 111;

    auto group = setUpGroup(appId, groupId, nThreads);

    // Set up another group
    int otherAppId = 22;
    int otherGroupId = 222;

    auto otherGroup = setUpGroup(otherAppId, otherGroupId, nThreads);

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
                 "[ptp][transport]")
{
    int nThreads = 4;
    int actual[4] = { 0, 0, 0, 0 };

    int appId = 11;
    int groupId = 111;

    auto group = setUpGroup(appId, groupId, nThreads);

    // Run threads in background to force a wait from the main
    std::vector<std::jthread> threads;
    for (int i = 1; i < nThreads; i++) {
        threads.emplace_back([&group, i, &actual] {
            SLEEP_MS(1000);
            actual[i] = i;

            group->notify(i);
        });
    }

    // Master thread to await, should only go through once all threads have
    // finished
    group->notify(POINT_TO_POINT_MASTER_IDX);

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
