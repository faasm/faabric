#include <faabric/executor/FaabricExecutor.h>
#include <faabric/executor/FaabricPool.h>
#include <faabric/util/logging.h>

namespace faabric::executor {
FaabricPool::FaabricPool(int nThreads)
  : _shutdown(false)
  , scheduler(faabric::scheduler::getScheduler())
  , threadTokenPool(nThreads)
  , stateServer(faabric::state::getGlobalState())
{

    // Ensure we can ping both redis instances
    faabric::redis::Redis::getQueue().ping();
    faabric::redis::Redis::getState().ping();
}

void FaabricPool::startFunctionCallServer()
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();
    logger->info("Starting function call server");
    functionServer.start(this->messageContext);
}

void FaabricPool::startSnapshotServer()
{
    auto logger = faabric::util::getLogger();
    logger->info("Starting snapshot server");
}

void FaabricPool::startStateServer()
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

    // Skip state server if not in inmemory mode
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    if (conf.stateMode != "inmemory") {
        logger->info("Not starting state server in state mode {}",
                     conf.stateMode);
        return;
    }

    // Note that the state server spawns its own background thread
    logger->info("Starting state server");
    stateServer.start(this->messageContext);
}

void FaabricPool::startThreadPool(bool background)
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();
    logger->info("Starting executor thread pool");

    // Spawn worker threads until we've hit the worker limit, thus creating a
    // pool that will replenish when one releases its token
    poolThread = std::thread([this] {
        const std::shared_ptr<spdlog::logger>& logger =
          faabric::util::getLogger();

        while (!this->isShutdown()) {
            // Try to get an available slot (blocks if none available)
            int threadIdx = this->getThreadToken();

            // Double check shutdown condition
            if (this->isShutdown()) {
                break;
            }

            // Spawn thread to execute function
            poolThreads.emplace_back(std::thread([this, threadIdx] {
                std::unique_ptr<FaabricExecutor> executor =
                  createExecutor(threadIdx);

                // Worker will now run for a long time
                try {
                    executor.get()->run();
                } catch (faabric::executor::ExecutorPoolFinishedException& e) {
                    this->_shutdown = true;
                }

                // Handle thread finishing
                threadTokenPool.releaseToken(executor.get()->threadIdx);
            }));
        }

        // Once shut down, wait for everything to die
        logger->info("Waiting for {} worker threads", poolThreads.size());
        for (auto& t : poolThreads) {
            if (t.joinable()) {
                t.join();
            }
        }

        // Will die gracefully at this point
    });

    // Block if in foreground
    if (!background) {
        poolThread.join();
    }
}

void FaabricPool::reset()
{
    threadTokenPool.reset();
}

int FaabricPool::getThreadToken()
{
    return threadTokenPool.getToken();
}

int FaabricPool::getThreadCount()
{
    return threadTokenPool.taken();
}

bool FaabricPool::isShutdown()
{
    return _shutdown;
}

void FaabricPool::shutdown()
{
    _shutdown = true;

    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

    logger->info("Waiting for the state server to finish");
    stateServer.stop(this->messageContext);

    logger->info("Waiting for the function server to finish");
    functionServer.stop(this->messageContext);

    if (poolThread.joinable()) {
        logger->info("Waiting for pool to finish");
        poolThread.join();
    }

    if (mpiThread.joinable()) {
        logger->info("Waiting for mpi thread to finish");
        mpiThread.join();
    }

    logger->info("Faabric pool successfully shut down");
}
}
