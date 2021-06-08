#include <faabric/endpoint/Endpoint.h>

#include <pistache/endpoint.h>
#include <pistache/listener.h>
#include <signal.h>
#include <spdlog/spdlog.h>

namespace faabric::endpoint {
Endpoint::Endpoint(int portIn, int threadCountIn)
  : port(portIn)
  , threadCount(threadCountIn)
{}

void Endpoint::start()
{
    SPDLOG_INFO("Starting HTTP endpoint");

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

    Pistache::Http::Endpoint httpEndpoint(addr);
    httpEndpoint.init(opts);

    // Configure and start endpoint
    httpEndpoint.setHandler(this->getHandler());
    httpEndpoint.serveThreaded();

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
