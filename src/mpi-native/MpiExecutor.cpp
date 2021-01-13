#include <faabric/mpi-native/MpiExecutor.h>

namespace faabric::executor {
MpiExecutor::MpiExecutor()
  : FaabricExecutor(0){};

bool MpiExecutor::doExecute(faabric::Message& msg)
{
    auto logger = faabric::util::getLogger();
    // TODO sanity check mpi message
    //faabric::Message& faabric::executor::executingCall = &msg;
    faabric::executor::executingCall = &msg;

    // Execute MPI code
    bool success = _execMpiFunc(&msg);
    return success;
}

void MpiExecutor::postFinishCall()
{
    auto logger = faabric::util::getLogger();
    logger->debug("Finished MPI execution.");
    // TODO shutdown everything
}

SingletonPool::SingletonPool()
  : FaabricPool(1)
  , scheduler(faabric::scheduler::getScheduler())
{
    auto logger = faabric::util::getLogger();
    auto conf = faabric::util::getSystemConfig();

    // Ensure we can ping both redis instances
    faabric::redis::Redis::getQueue().ping();
    faabric::redis::Redis::getState().ping();

    // Add host to the list of global sets and print configuration
    this->scheduler.addHostToGlobalSet();
    conf.print();
}

SingletonPool::~SingletonPool()
{
    auto logger = faabric::util::getLogger();

    logger->debug("Destructor for singleton pool.");
    // scheduler.clear();
    this->shutdown();
    // TODO finish endpoint
}

void SingletonPool::startPool()
{
    // Start singleton thread pool
    this->startThreadPool();
    this->startStateServer();
    this->startFunctionCallServer();
    this->endpoint.start();
}
}
