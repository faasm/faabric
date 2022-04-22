#pragma once

#include "faabric_utils.h"

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
        sch.removeHostFromGlobalSet(LOCALHOST);
        sch.addHostToGlobalSet(getMasterIP());
        sch.addHostToGlobalSet(getWorkerIP());

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
        // Flush before each execution to ensure a clean start
        sch.broadcastFlush();
        SLEEP_MS(INTER_MPI_TEST_SLEEP);
    }

    ~MpiDistTestsFixture() = default;

  protected:
    int nLocalSlots = 2;
    int worldSize = 4;

    // The server has four slots, therefore by setting the number of local slots
    // and the world size we are able to infer the expected scheduling decision
    void setLocalSlots(int numSlots, int worldSizeIn = 0)
    {
        faabric::HostResources res;
        res.set_slots(numSlots);
        sch.setThisHostResources(res);

        if (worldSizeIn > 0) {
            worldSize = worldSizeIn;
        }
    }

    std::shared_ptr<faabric::BatchExecuteRequest> setRequest(
      const std::string& function)
    {
        auto req = faabric::util::batchExecFactory("mpi", function, 1);
        faabric::Message& msg = req->mutable_messages()->at(0);
        msg.set_mpiworldsize(worldSize);
        msg.set_recordexecgraph(true);

        return req;
    }

    void checkSchedulingFromExecGraph(
      const faabric::scheduler::ExecGraph& execGraph)
    {
        // Build the expectation
        // Note - here we assume that MPI functions DON'T use the `NEVER_ALONE`
        // scheduling topology hint. TODO change when #184 is merged in
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
        int alocateRemotely =
          std::min<int>(worldSize - nLocalSlots, remoteSlots);
        for (int i = 0; i < alocateRemotely; i++) {
            expecedHosts.push_back(remoteIp);
        }
        // Lastly, overload the master with all the ranks we haven't been able
        // to allocate anywhere
        int overloadMaster = worldSize - nLocalSlots - alocateRemotely;
        for (int i = 0; i < overloadMaster; i++) {
            expecedHosts.push_back(localIp);
        }

        // Check against the actual scheduling decision
        REQUIRE(expecedHosts ==
                faabric::scheduler::getMpiRankHostsFromExecGraph(execGraph));
    }

    void checkAllocationAndResult(
      std::shared_ptr<faabric::BatchExecuteRequest> req)
    {
        faabric::Message& msg = req->mutable_messages()->at(0);
        faabric::Message result = sch.getFunctionResult(msg.id(), 1000);
        REQUIRE(result.returnvalue() == 0);
        // TODO - remove this sleep when #181 is merged and rebased
        SLEEP_MS(1000);
        auto execGraph = sch.getFunctionExecGraph(msg.id());
        checkSchedulingFromExecGraph(execGraph);
    }
};
}
