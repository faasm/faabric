#include <faabric/util/logging.h>

#include <faabric/executor/FaabricMain.h>
#include <faabric/scheduler/Scheduler.h>

#define EXAMPLE_BUILD_PATH "/code/faabric/examples/build"

// Macro defined in include/faabric/executor/FaabricPool.h
// bool _execFunc(faabric::Message& msg)
FAABRIC_EXECUTOR
{
    // logger->info("Executor {} running function {}"
    std::string function = msg.function();
    std::string function_path =
      std::string(EXAMPLE_BUILD_PATH) + "/" + function;
    return true;
}

int main(int argc, char** argv)
{
    // Process input parameters

    faabric::util::initLogging();
    auto logger = faabric::util::getLogger();

    logger->info("Starting faaslet pool in the background");
    _Pool p(5);
    FaabricMain w(p);
    w.startBackground();

    logger->info("Running mpi function {}", function);
    const char* funcName = "hellompi";
    faabric::Message msg = faabric::util::messageFactory("mpi", funcName);
    msg.set_mpiworldsize(5);
    auto sch = faabric::scheduler::getScheduler();
    sch.callFunction(msg);

    return 0;
}
