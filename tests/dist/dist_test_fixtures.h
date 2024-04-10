#pragma once

#include "faabric_utils.h"
#include "fixtures.h"

#include "DistTestExecutor.h"

#include <faabric/executor/ExecutorFactory.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotRegistry.h>

#define INTER_MPI_TEST_SLEEP 1500

namespace tests {
class DistTestsFixture
  : public SchedulerFixture
  , public ConfFixture
  , public SnapshotRegistryFixture
  , public PointToPointBrokerFixture
{
  public:
    DistTestsFixture()
    {
        // Reset the planner
        resetPlanner();

        // Make sure the host list is up to date
        sch.addHostToGlobalSet(getMasterIP());
        sch.addHostToGlobalSet(getWorkerIP());
        sch.removeHostFromGlobalSet(LOCALHOST);

        // Give some resources to each host
        updateLocalSlots(4, 0);
        updateRemoteSlots(4, 0);

        // The dist-test server always uses at most 4 slots, so we configure
        // the main worker (here) to start assigning CPU cores from core 4
        conf.overrideFreeCpuStart = 4;

        // Set up executor
        std::shared_ptr<tests::DistTestExecutorFactory> fac =
          std::make_shared<tests::DistTestExecutorFactory>();
        faabric::executor::setExecutorFactory(fac);
    }

    void updateLocalSlots(int newLocalSlots, int newUsedLocalSlots = 0)
    {
        faabric::HostResources localRes;
        localRes.set_slots(newLocalSlots);
        localRes.set_usedslots(newUsedLocalSlots);
        sch.setThisHostResources(localRes);
    }

    void updateRemoteSlots(int newRemoteSlots, int newRemoteUsedSlots = 0)
    {
        faabric::HostResources remoteRes;
        remoteRes.set_slots(newRemoteSlots);
        remoteRes.set_usedslots(newRemoteUsedSlots);
        sch.addHostToGlobalSet(workerIP,
                               std::make_shared<HostResources>(remoteRes));
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

  protected:
    std::string workerIP;
    std::string mainIP;
};

class MpiDistTestsFixture : public DistTestsFixture
{
  public:
    MpiDistTestsFixture() {}

    ~MpiDistTestsFixture() = default;

  protected:
    int nLocalSlots = 2;
    int worldSize = 4;
    bool origIsMsgOrderingOn;

    void setLocalSlots(int numLocalSlots, int worldSizeIn = 0)
    {
        if (worldSizeIn > 0) {
            worldSize = worldSizeIn;
        }
        int numRemoteSlots = worldSize - numLocalSlots;

        if (numLocalSlots == numRemoteSlots) {
            updateLocalSlots(2 * numLocalSlots, numLocalSlots);
            updateRemoteSlots(numRemoteSlots);
        } else if (numLocalSlots > numRemoteSlots) {
            updateLocalSlots(numLocalSlots);
            updateRemoteSlots(numRemoteSlots);
        } else {
            SPDLOG_ERROR(
              "Unfeasible MPI world slots config (local: {} - remote: {})",
              numLocalSlots,
              numRemoteSlots);
            throw std::runtime_error("Unfeasible slots configuration");
        }
    }

    std::shared_ptr<faabric::BatchExecuteRequest> setRequest(
      const std::string& function) const
    {
        auto req = faabric::util::batchExecFactory("mpi", function, 1);
        faabric::Message& msg = req->mutable_messages()->at(0);
        msg.set_ismpi(true);
        msg.set_mpiworldsize(worldSize);

        return req;
    }

    std::vector<std::string> waitForMpiMessagesInFlight(
      std::shared_ptr<faabric::BatchExecuteRequest> req)
    {
        // Wait until all the messages have been scheduled
        int maxRetries = 20;
        int numRetries = 0;
        int pollSleepMs = 500;
        auto decision = plannerCli.getSchedulingDecision(req);
        while (decision.messageIds.size() != worldSize) {
            if (numRetries >= maxRetries) {
                SPDLOG_ERROR("Timed-out waiting for MPI messages to be "
                             "scheduled (app: {}, {}/{})",
                             req->appid(),
                             decision.messageIds.size(),
                             worldSize);
                throw std::runtime_error("Timed-out waiting for MPI messges");
            }

            SLEEP_MS(pollSleepMs);
            decision = plannerCli.getSchedulingDecision(req);
            numRetries += 1;
        }

        return decision.hosts;
    }

    void checkAllocationAndResult(
      std::shared_ptr<faabric::BatchExecuteRequest> req)
    {
        checkAllocationAndResult(
          req, { getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP() });
    }

    void checkAllocationAndResult(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const std::vector<std::string>& expectedHosts)
    {
        int numRetries = 0;
        int maxRetries = 100;
        int pollSleepMs = 250;
        auto batchResults = plannerCli.getBatchResults(req);
        while (batchResults->messageresults_size() != worldSize) {
            if (numRetries >= maxRetries) {
                SPDLOG_ERROR(
                  "Timed-out waiting for MPI messages results (app: {}, {}/{})",
                  req->appid(),
                  batchResults->messageresults_size(),
                  worldSize);
                throw std::runtime_error("Timed-out waiting for MPI messges");
            }

            SLEEP_MS(pollSleepMs);
            batchResults = plannerCli.getBatchResults(req);
            numRetries += 1;
        }

        for (const auto& msg : batchResults->messageresults()) {
            REQUIRE(msg.returnvalue() == 0);
            REQUIRE(expectedHosts.at(msg.mpirank()) == msg.executedhost());
        }
    }
};
}
