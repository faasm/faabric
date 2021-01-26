#include <faabric/executor/FaabricMain.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

#if(FAASM_SGX)
namespace sgx {
	    extern void checkSgxSetup();
}
#endif

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

#if(FAASM_SGX)
	// Check for SGX capability and create shared enclave
	sgx::checkSgxSetup();
#endif

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

    scheduler.shutdown();

    pool.shutdown();
}
}
