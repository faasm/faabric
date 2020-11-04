#include "FaabricMain.h"

#include <faabric/util/config.h>
#include <faabric/util/logging.h>

namespace faabric::executor {
FaabricMain::FaabricMain(faabric::executor::FaabricPool& poolIn)
  : conf(faabric::util::getSystemConfig())
  , scheduler(faabric::scheduler::getScheduler())
  , pool(poolIn)
{}

void FaabricMain::startBackground()
{
    scheduler.addHostToGlobalSet();

    conf.print();

    // Start thread pool in background
    pool.startThreadPool();

    // In-memory state
    pool.startStateServer();

    // Work sharing
    pool.startFunctionCallServer();
}

void FaabricMain::shutdown()
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();
    logger->info("Removing from global working set");

    scheduler.clear();

    pool.shutdown();
}
}

