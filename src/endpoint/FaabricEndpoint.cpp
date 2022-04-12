#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/endpoint/FaabricEndpointHandler.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
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

void FaabricEndpoint::start(EndpointMode mode)
{
    sigset_t signals;

    if (mode == EndpointMode::SIGNAL) {
        SPDLOG_INFO(
          "Starting blocking endpoint on {}, {} threads", port, threadCount);

        // Set up signal handler
        if (sigemptyset(&signals) != 0 || sigaddset(&signals, SIGTERM) != 0 ||
            sigaddset(&signals, SIGKILL) != 0 ||
            sigaddset(&signals, SIGINT) != 0 ||
            sigaddset(&signals, SIGHUP) != 0 ||
            sigaddset(&signals, SIGQUIT) != 0 ||
            pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) {

            throw std::runtime_error("Install signal handler failed");
        }
    } else if (mode == EndpointMode::BG_THREAD) {
        SPDLOG_INFO(
          "Starting background endpoint on {}, {} threads", port, threadCount);
    } else {
    }

    // Configure endpoint, locking to keep thread sanitiser happy
    {
        faabric::util::UniqueLock lock(mx);
        auto opts = Pistache::Http::Endpoint::options()
                      .threads(threadCount)
                      .backlog(256)
                      .flags(Pistache::Tcp::Options::ReuseAddr);

        httpEndpoint.init(opts);

        // Configure and start endpoint
        httpEndpoint.setHandler(
          Pistache::Http::make_handler<FaabricEndpointHandler>());
        httpEndpoint.serveThreaded();
    }

    if (mode == EndpointMode::SIGNAL) {
        // Wait for a signal
        SPDLOG_INFO("Awaiting signal");
        int signal = 0;
        int status = sigwait(&signals, &signal);
        if (status == 0) {
            SPDLOG_INFO("Received signal: {}", signal);
        } else {
            SPDLOG_INFO("Sigwait return value: {}", signal);
        }

        faabric::util::UniqueLock lock(mx);
        httpEndpoint.shutdown();
    }
}

void FaabricEndpoint::stop()
{
    SPDLOG_INFO("Shutting down endpoint on {}", port);
    faabric::util::UniqueLock lock(mx);
    httpEndpoint.shutdown();
}
}
