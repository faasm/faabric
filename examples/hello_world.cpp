#include <faabric/util/logging.h>

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/executor/FaabricMain.h>

using namespace faabric::executor;

FAABRIC_EXECUTOR()
{
    auto logger = faabric::util::getLogger();

    logger->info("Hello world!");
    msg.set_outputdata("This is hello output!");

    return true;
}

int main()
{
    faabric::util::initLogging();
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

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
