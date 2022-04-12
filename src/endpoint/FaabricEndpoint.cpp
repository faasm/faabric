#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/endpoint/FaabricEndpointHandler.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#include <signal.h>

namespace faabric::endpoint {

FaabricEndpoint::FaabricEndpoint()
  : FaabricEndpoint(faabric::util::getSystemConfig().endpointPort,
                    faabric::util::getSystemConfig().endpointNumThreads)
{}

FaabricEndpoint::FaabricEndpoint(int portIn, int threadCountIn)
  : port(portIn)
  , threadCount(threadCountIn)
  , httpEndpoint(
      Pistache::Address(Pistache::Ipv4::any(), Pistache::Port(portIn)))
{}

std::shared_ptr<Pistache::Http::Handler> FaabricEndpoint::getHandler()
{
    return Pistache::Http::make_handler<FaabricEndpointHandler>();
}

void FaabricEndpoint::start(bool awaitSignal)
{
    SPDLOG_INFO("Starting HTTP endpoint on {}, {} threads", port, threadCount);

    // Set up signal handler
    sigset_t signals;
    if (awaitSignal) {
        if (sigemptyset(&signals) != 0 || sigaddset(&signals, SIGTERM) != 0 ||
            sigaddset(&signals, SIGKILL) != 0 ||
            sigaddset(&signals, SIGINT) != 0 ||
            sigaddset(&signals, SIGHUP) != 0 ||
            sigaddset(&signals, SIGQUIT) != 0 ||
            pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) {

            throw std::runtime_error("Install signal handler failed");
        }
    }

    // Configure endpoint
    auto opts = Pistache::Http::Endpoint::options()
                  .threads(threadCount)
                  .backlog(256)
                  .flags(Pistache::Tcp::Options::ReuseAddr);

    httpEndpoint.init(opts);

    // Configure and start endpoint
    httpEndpoint.setHandler(this->getHandler());
    httpEndpoint.serveThreaded();

    if (awaitSignal) {
        // Wait for a signal
        SPDLOG_INFO("Awaiting signal");
        int signal = 0;
        int status = sigwait(&signals, &signal);
        if (status == 0) {
            SPDLOG_INFO("Received signal: {}", signal);
        } else {
            SPDLOG_INFO("Sigwait return value: {}", signal);
        }

        httpEndpoint.shutdown();
    }
}

void FaabricEndpoint::stop()
{
    SPDLOG_INFO("Shutting down endpoint on {}", port);
    httpEndpoint.shutdown();
}
}
