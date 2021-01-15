#include <faabric/mpi-native/MpiExecutor.h>

namespace faabric::executor {
faabric::Message* executingCall;
int mpiFunc();

MpiExecutor::MpiExecutor()
  : FaabricExecutor(0){};

bool MpiExecutor::doExecute(faabric::Message& msg)
{
    auto logger = faabric::util::getLogger();

    faabric::executor::executingCall = &msg;

    bool success;
    int error = mpiFunc();
    if (error) {
        logger->error("There was an error running the MPI function");
        success = false;
    } else {
        success = true;
    }

    return success;
}

void MpiExecutor::postFinishCall()
{
    auto logger = faabric::util::getLogger();
    logger->debug("Finished MPI execution.");
    // TODO shutdown everything
    // Stops function server, call server, and thread pool
    // singletonPool->shutdown();
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
    // Stops function server and call server
    // this->shutdown();
    // TODO finish endpoint
}

void SingletonPool::startPool(bool background)
{
    // Start singleton thread pool
    this->startThreadPool();
    this->startStateServer();
    this->startFunctionCallServer();
    this->endpoint.start(background);
}
}
