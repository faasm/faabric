#include <faabric/util/logging.h>

#include <faabric/executor/FaabricMain.h>
#include <faabric/endpoint/FaabricEndpoint.h>

using namespace faabric::endpoint;
using namespace faabric::executor;

int main() {
    faabric::util::initLogging();
    const std::shared_ptr<spdlog::logger> &logger = faabric::util::getLogger();

    // TODO - template the FaabricMain with user-defined <ExecutorType>
    // TODO - wrap up even more of this into a convenience function?

    // Start the worker pool
    logger->info("Starting faaslet pool in the background");
    FaabricMain w;
    w.startBackground();

    // Start endpoint (will also have multiple threads)
    logger->info("Starting endpoint");
    FaabricEndpoint endpoint;
    endpoint.start();

    logger->info("Shutting down endpoint");
    w.shutdown();

    return EXIT_SUCCESS;
}
