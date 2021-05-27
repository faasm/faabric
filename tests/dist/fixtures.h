#pragma once

#include "DistTestExecutor.h"

#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotRegistry.h>

namespace tests {
class DistTestsFixture
{
  protected:
    faabric::scheduler::Scheduler& sch;
    faabric::util::SystemConfig& conf;
    faabric::snapshot::SnapshotRegistry& reg;

    std::set<std::string> otherHosts;

  public:
    DistTestsFixture()
      : sch(faabric::scheduler::getScheduler())
      , conf(faabric::util::getSystemConfig())
      , reg(faabric::snapshot::getSnapshotRegistry())
    {
        // Make sure this host is available
        sch.addHostToGlobalSet();

        // Clear local snapshot info
        reg.clear();

        // Get other hosts
        std::string thisHost = conf.endpointHost;
        otherHosts = sch.getAvailableHosts();
        otherHosts.erase(thisHost);

        // Set up executor
        std::shared_ptr<tests::DistTestExecutorFactory> fac =
          std::make_shared<tests::DistTestExecutorFactory>();
        faabric::scheduler::setExecutorFactory(fac);
    }

    ~DistTestsFixture()
    {
        reg.clear();
        sch.reset();
        conf.reset();
    }
};
}
