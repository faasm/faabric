#include <faabric/util/logging.h>

#include <faabric/executor/FaabricMain.h>
#include <faabric/endpoint/FaabricEndpoint.h>

using namespace faabric::executor;

FAABRIC_EXECUTOR() {
    faabric::util::getLogger()->debug("Executing {}/{}", msg.user(), msg.function());
    return true;
}

int main() {
    faabric::util::initLogging();
    const std::shared_ptr<spdlog::logger> &logger = faabric::util::getLogger();

    // TODO - template the FaabricMain with user-defined <ExecutorType>
    // TODO - wrap up even more of this into a convenience function?

    // Start the worker pool
    logger->info("Starting faaslet pool in the background");
    _Pool p(5);
    FaabricMain w(p);
    w.startBackground();

    // Start endpoint (will also have multiple threads)
    logger->info("Starting endpoint");
    faabric::endpoint::FaabricEndpoint endpoint;
    endpoint.start();

    logger->info("Shutting down endpoint");
    w.shutdown();

    return EXIT_SUCCESS;
}
