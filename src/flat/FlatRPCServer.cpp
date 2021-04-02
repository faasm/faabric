#include <faabric/flat/FlatRPCServer.h>
#include <faabric/util/logging.h>
#include <grpcpp/grpcpp.h>

using namespace faabric::util;

namespace faabric::rpc {
FlatRPCServer::FlatRPCServer(const std::string& hostIn, int portIn)
  : host(hostIn)
  , port(portIn)
{}

void FlatRPCServer::start(bool background)
{
    auto logger = getLogger();
    std::string serverAddr = host + ":" + std::to_string(port);

    _started = true;
    _isBackground = background;

    if (background) {
        logger->debug("Starting flatbuf RPC server in background thread");
        // Run the serving thread in the background. This is necessary to
        // be able to kill it from the main thread.
        servingThread =
          std::thread([this, serverAddr] { doStart(serverAddr); });

    } else {
        logger->debug("Starting flatbuf RPC server in this thread");
        doStart(serverAddr);
    }
}

void FlatRPCServer::stop()
{
    const std::shared_ptr<spdlog::logger>& logger = getLogger();
    if (!_started) {
        logger->info("Not stopping flatbuf RPC server, never started");
        return;
    }

    logger->info("flatbuf RPC server stopping");
    server->Shutdown();

    if (_isBackground) {
        logger->debug("Waiting for flatbuf RPC server background thread");
        if (servingThread.joinable()) {
            servingThread.join();
        }
    }
}
}
