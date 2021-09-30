#pragma once

#include "faabric_utils.h"

#include "DistTestExecutor.h"

#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotRegistry.h>

namespace tests {
class DistTestsFixture
  : public SchedulerTestFixture
  , public ConfTestFixture
  , public SnapshotTestFixture
{
  public:
    DistTestsFixture()
    {
        // Get other hosts
        std::string thisHost = conf.endpointHost;

        // Set up executor
        std::shared_ptr<tests::DistTestExecutorFactory> fac =
          std::make_shared<tests::DistTestExecutorFactory>();
        faabric::scheduler::setExecutorFactory(fac);
    }

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
}
