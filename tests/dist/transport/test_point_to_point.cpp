#include <catch2/catch.hpp>

#include "dist_test_fixtures.h"
#include "faabric_utils.h"
#include "init.h"

#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

namespace tests {

class PointToPointDistTestFixture : public DistTestsFixture
{
  public:
    PointToPointDistTestFixture()
    {
        // Check the available hosts
        auto availableHosts = plannerCli.getAvailableHosts();
        std::set<std::string> actualAvailable;
        for (auto& host : availableHosts) {
            actualAvailable.insert(host.ip());
        }
        std::set<std::string> expectedAvailable = { getMasterIP(),
                                                    getWorkerIP() };
        REQUIRE(actualAvailable == expectedAvailable);
    }

    ~PointToPointDistTestFixture() = default;

    void setSlotsAndNumFuncs(int nLocalSlotsIn, int nFuncsIn)
    {
        nLocalSlots = nLocalSlotsIn;
        nFuncs = nFuncsIn;

        // Set local resources
        faabric::HostResources res;
        res.set_slots(nLocalSlots);
        sch.setThisHostResources(res);
    }

    faabric::batch_scheduler::SchedulingDecision prepareRequestReturnDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req)
    {
        // Prepare expected decision
        faabric::batch_scheduler::SchedulingDecision expectedDecision(appId,
                                                                      groupId);
        std::vector<std::string> expectedHosts(nFuncs, getWorkerIP());
        for (int i = 0; i < nLocalSlots; i++) {
            expectedHosts.at(i) = getMasterIP();
        }

        // Set up individual messages
        for (int i = 0; i < nFuncs; i++) {
            faabric::Message& msg = req->mutable_messages()->at(i);

            msg.set_appid(appId);
            msg.set_appidx(i);
            msg.set_groupid(groupId);
            msg.set_groupidx(i);

            // Add to expected decision
            expectedDecision.addMessage(expectedHosts.at(i),
                                        req->messages().at(i));
        }

        return expectedDecision;
    }

    void checkReturnCodesAndSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::batch_scheduler::SchedulingDecision& expectedDecision,
      faabric::batch_scheduler::SchedulingDecision& actualDecision)
    {
        checkSchedulingDecisionEquality(actualDecision, expectedDecision);

        // Check functions executed successfully
        for (int i = 0; i < nFuncs; i++) {
            faabric::Message& m = req->mutable_messages()->at(i);

            plannerCli.getMessageResult(m, 2000);
            REQUIRE(m.returnvalue() == 0);
        }
    }

  protected:
    int appId = 222;
    int groupId = 333;

    int nLocalSlots;
    int nFuncs;
};

TEST_CASE_METHOD(PointToPointDistTestFixture,
                 "Test point-to-point messaging on multiple hosts",
                 "[ptp][transport]")
{
    setSlotsAndNumFuncs(1, 4);

    // Set up batch request and scheduling decision
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("ptp", "simple", nFuncs);
    faabric::batch_scheduler::SchedulingDecision expectedDecision =
      prepareRequestReturnDecision(req);

    // Call the functions
    auto actualDecision = plannerCli.callFunctions(req);

    // Check for equality
    checkReturnCodesAndSchedulingDecision(
      req, expectedDecision, actualDecision);
}

TEST_CASE_METHOD(PointToPointDistTestFixture,
                 "Test many in-order point-to-point messages",
                 "[ptp][transport]")
{
    setSlotsAndNumFuncs(1, 2);

    // Set up batch request
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("ptp", "many-msg", nFuncs);
    faabric::batch_scheduler::SchedulingDecision expectedDecision =
      prepareRequestReturnDecision(req);

    // Call the functions
    auto actualDecision = plannerCli.callFunctions(req);

    // Check for equality
    checkReturnCodesAndSchedulingDecision(
      req, expectedDecision, actualDecision);
}

TEST_CASE_METHOD(DistTestsFixture,
                 "Test distributed coordination",
                 "[ptp][transport]")
{
    // Set up this host's resources, force execution across hosts
    int nChainedFuncs = 4;
    int nLocalSlots = 2;

    faabric::HostResources res;
    res.set_slots(nLocalSlots);
    sch.setThisHostResources(res);

    std::string function;
    SECTION("Barrier") { function = "barrier"; }

    SECTION("Notify") { function = "notify"; }

    // Set up the message
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("ptp", function, 1);

    // Set number of chained funcs
    faabric::Message& m = req->mutable_messages()->at(0);
    m.set_inputdata(std::to_string(nChainedFuncs));

    // Call the function
    std::vector<std::string> expectedHosts = { getMasterIP() };
    std::vector<std::string> executedHosts = plannerCli.callFunctions(req).hosts;
    REQUIRE(expectedHosts == executedHosts);

    // Get result
    faabric::Message result = plannerCli.getMessageResult(m, 10000);
    REQUIRE(result.returnvalue() == 0);
}
}
