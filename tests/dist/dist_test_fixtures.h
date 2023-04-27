#pragma once

#include "faabric_utils.h"
#include "fixtures.h"

#include "DistTestExecutor.h"

#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotRegistry.h>

#define INTER_MPI_TEST_SLEEP 1500

namespace tests {
class DistTestsFixture
  : public SchedulerTestFixture
  , public ConfTestFixture
  , public SnapshotTestFixture
  , public PointToPointTestFixture
{
  public:
    DistTestsFixture()
    {
        // Make sure the host list is up to date
        sch.addHostToGlobalSet(getMasterIP());
        // Even though the worker will send keep-alive messages, we reset the
        // planner as part of the dist-test fixture. Thus, the period of time
        // between the reset, and the keep-alive, the planner is not aware
        // of the worker host. Thus, we add it here
        auto workerResources = std::make_shared<faabric::HostResources>();
        workerResources->set_slots(4);
        sch.addHostToGlobalSet(getWorkerIP(), workerResources);

        // Set up executor
        std::shared_ptr<tests::DistTestExecutorFactory> fac =
          std::make_shared<tests::DistTestExecutorFactory>();
        faabric::scheduler::setExecutorFactory(fac);
    }

    ~DistTestsFixture() = default;

    std::string getWorkerIP()
    {
        if (workerIP.empty()) {
            workerIP = faabric::util::getIPFromHostname("dist-test-server");
        }

        return workerIP;
    }

    std::string getMasterIP() { return conf.endpointHost; }

  private:
    std::string workerIP;
    std::string masterIP;
};

class MpiDistTestsFixture : public DistTestsFixture
{
  public:
    MpiDistTestsFixture()
    {
        // TODO: do we need this?
        SLEEP_MS(INTER_MPI_TEST_SLEEP);
    }

    ~MpiDistTestsFixture() = default;

  protected:
    int nLocalSlots = 4;
    int worldSize = 8;
    bool origIsMsgOrderingOn;

    // The server has four slots, therefore by setting the number of local slots
    // and the world size we are able to infer the expected scheduling decision
    void setLocalSlots(int numSlots, int worldSizeIn = 0)
    {
        faabric::HostResources res;
        res.set_slots(2 * numSlots);
        res.set_usedslots(numSlots);
        sch.setThisHostResources(res);

        if (worldSizeIn > 0) {
            worldSize = worldSizeIn;
        }
    }

    void setRemoteSlots(int numSlots, int numUsedSlots = 0)
    {
        auto res = std::make_shared<faabric::HostResources>();
        res->set_slots(numSlots);
        res->set_usedslots(numUsedSlots);

        sch.addHostToGlobalSet(getWorkerIP(), res);
    }

    std::shared_ptr<faabric::BatchExecuteRequest> setRequest(
      const std::string& function)
    {
        auto req = faabric::util::batchExecFactory("mpi", function, 1);
        faabric::Message& msg = req->mutable_messages()->at(0);
        msg.set_ismpi(true);
        msg.set_mpiworldsize(worldSize);

        return req;
    }

    std::vector<std::string> buildExpectedHosts()
    {
        // Build the expectation
        std::vector<std::string> expecedHosts;
        // The distributed server is hardcoded to have four slots
        int remoteSlots = 4;
        std::string remoteIp = getWorkerIP();
        std::string localIp = getMasterIP();
        // First, allocate locally as much as we can
        for (int i = 0; i < nLocalSlots; i++) {
            expecedHosts.push_back(localIp);
        }
        // Second, allocate remotely as much as we can
        int allocateRemotely =
          std::min<int>(worldSize - nLocalSlots, remoteSlots);
        for (int i = 0; i < allocateRemotely; i++) {
            expecedHosts.push_back(remoteIp);
        }
        // Lastly, overload the master with all the ranks we haven't been able
        // to allocate anywhere
        int overloadMaster = worldSize - nLocalSlots - allocateRemotely;
        for (int i = 0; i < overloadMaster; i++) {
            expecedHosts.push_back(localIp);
        }

        return expecedHosts;
    }

    void checkAllocationAndResult(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      int timeoutMs = 10000,
      std::vector<std::string> expectedHosts = {})
    {
        /*
        for (const auto& msg : req->messages()) {
            auto result = sch.getFunctionResult(msg, timeoutMs);
            REQUIRE(result.returnvalue() == 0);
        }
        int numRetries = 3;
        for (int i = 0; i < numRetries; i++) {
        */

        // Sleep for a bit to make sure the scheduler has had time to schedule
        // all MPI calls before we wait on the batch
        SLEEP_MS(200);

        int batchSizeHint =
          expectedHosts.empty() ? worldSize : expectedHosts.size();
        auto responseReq = sch.getBatchResult(req, timeoutMs, batchSizeHint);
        std::vector<std::string> actualHosts(responseReq->messages_size());
        for (const auto& msg : responseReq->messages()) {
            REQUIRE(msg.returnvalue() == 0);
            actualHosts.at(msg.groupidx()) = msg.executedhost();
        }

        if (expectedHosts.empty()) {
            expectedHosts = buildExpectedHosts();
        }

        REQUIRE(actualHosts == expectedHosts);
    }
};
}
