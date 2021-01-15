#include <faabric/endpoint/Endpoint.h>

#include <faabric/util/logging.h>
#include <pistache/endpoint.h>
#include <pistache/listener.h>
#include <signal.h>

namespace faabric::endpoint {
Endpoint::Endpoint(int portIn, int threadCountIn)
  : port(portIn)
  , threadCount(threadCountIn)
{}

void Endpoint::start(bool background)
{
    auto logger = faabric::util::getLogger();
    this->isBackground = background;

    if (background) {
        logger->debug("Starting HTTP endpoint in background thread");
        servingThread = std::thread([this] { doStart(); });
    } else {
        logger->debug("Starting HTTP endpoint in this thread");
        doStart();
    }
}

void Endpoint::doStart()
{
    auto logger = faabric::util::getLogger();

    // Set up signal handler
    sigset_t signals;
    if (sigemptyset(&signals) != 0 || sigaddset(&signals, SIGTERM) != 0 ||
        sigaddset(&signals, SIGKILL) != 0 || sigaddset(&signals, SIGINT) != 0 ||
        sigaddset(&signals, SIGHUP) != 0 || sigaddset(&signals, SIGQUIT) != 0 ||
        pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) {

        throw std::runtime_error("Install signal handler failed");
    }

    Pistache::Address addr(Pistache::Ipv4::any(), Pistache::Port(this->port));

    // Configure endpoint
    auto opts = Pistache::Http::Endpoint::options()
                  .threads(threadCount)
                  .backlog(256)
                  .flags(Pistache::Tcp::Options::ReuseAddr);

    httpEndpoint = std::make_unique<Pistache::Http::Endpoint>(addr);
    httpEndpoint->init(opts);

    // Configure and start endpoint
    httpEndpoint->setHandler(this->getHandler());
    httpEndpoint->serveThreaded();

    // Wait for a signal if running in foreground (this blocks execution)
    if (!isBackground) {
        logger->info("Awaiting signal");
        int signal = 0;
        int status = sigwait(&signals, &signal);
        if (status == 0) {
            logger->info("Received signal: {}", signal);
        } else {
            logger->info("Sigwait return value: {}", signal);
        }

        stop();
    }
}

void Endpoint::stop()
{
    // Shut down the endpoint
    auto logger = faabric::util::getLogger();

    logger->debug("Shutting down HTTP endpoint");
    this->httpEndpoint->shutdown();

    if (isBackground) {
        logger->debug("Waiting for HTTP endpoint background thread");
        if (servingThread.joinable()) {
            servingThread.join();
        }
    }
}
}
