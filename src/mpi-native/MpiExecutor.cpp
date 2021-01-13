#include <faabric/mpi-native/MpiExecutor.h>

#include <unistd.h>

namespace faabric::executor {
faabric::Message* executingCall;
thread_local std::function<bool(faabric::Message*)> mpiFunc;

MpiExecutor::MpiExecutor()
  : FaabricExecutor(0){};

bool MpiExecutor::doExecute(faabric::Message& msg)
{
    auto logger = faabric::util::getLogger();

    faabric::executor::executingCall = &msg;

    // TODO delete 
    sleep(10);

    return true;
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
    this->endpoint.start(true);
}
}
