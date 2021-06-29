#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/common.h>
#include <faabric/util/latch.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>

#include <csignal>
#include <cstdlib>

namespace faabric::transport {

static const std::vector<uint8_t> shutdownHeader = { 0, 0, 1, 1 };

#define SHUTDOWN_CHECK(header, label)                                          \
    {                                                                          \
        if (header.size() == shutdownHeader.size()) {                          \
            if (header.dataCopy() == shutdownHeader) {                         \
                SPDLOG_TRACE("Server {} endpoint received shutdown message",   \
                             label);                                           \
                break;                                                         \
            }                                                                  \
        }                                                                      \
    }

#define RECEIVE_BODY(header, endpoint)                                         \
    if (!header.more()) {                                                      \
        throw std::runtime_error("Header sent without SNDMORE flag");          \
    }                                                                          \
    Message body = endpoint.recv();                                            \
    if (body.more()) {                                                         \
        throw std::runtime_error("Body sent with SNDMORE flag");               \
    }

MessageEndpointServer::MessageEndpointServer(int asyncPortIn, int syncPortIn)
  : asyncPort(asyncPortIn)
  , syncPort(syncPortIn)
  , asyncShutdownSender(LOCALHOST, asyncPort)
  , syncShutdownSender(LOCALHOST, syncPort)
{}

void MessageEndpointServer::start()
{
    // This latch means that callers can guarantee that when this function
    // completes, both sockets will have been opened (and hence the server is
    // ready to use).
    faabric::util::Latch startLatch(3);

    asyncThread = std::thread([this, &startLatch] {
        AsyncRecvMessageEndpoint endpoint(asyncPort);
        startLatch.wait();

        while (true) {
            // Receive header and body
            Message header = endpoint.recv();

            SHUTDOWN_CHECK(header, "async")

            RECEIVE_BODY(header, endpoint)

            // Server-specific message handling
            doAsyncRecv(header, body);

            // Wait on the async latch if necessary
            if (asyncLatch != nullptr) {
                SPDLOG_TRACE(
                  "Server thread waiting on async latch for port {}",
                  asyncPort);
                asyncLatch->wait();
            }
        }
    });

    syncThread = std::thread([this, &startLatch] {
        SyncRecvMessageEndpoint endpoint(syncPort);
        startLatch.wait();

        while (true) {
            // Receive header and body
            Message header = endpoint.recv();

            SHUTDOWN_CHECK(header, "sync")

            RECEIVE_BODY(header, endpoint)

            // Server-specific message handling
            std::unique_ptr<google::protobuf::Message> resp =
              doSyncRecv(header, body);
            size_t respSize = resp->ByteSizeLong();

            uint8_t buffer[respSize];
            if (!resp->SerializeToArray(buffer, respSize)) {
                throw std::runtime_error("Error serialising message");
            }

            endpoint.sendResponse(buffer, respSize);
        }
    });

    startLatch.wait();
}

void MessageEndpointServer::stop()
{
    // Send shutdown messages
    SPDLOG_TRACE(
      "Server sending shutdown messages to ports {} {}", asyncPort, syncPort);

    asyncShutdownSender.send(shutdownHeader.data(), shutdownHeader.size());

    syncShutdownSender.sendRaw(shutdownHeader.data(), shutdownHeader.size());

    // Join the threads
    if (asyncThread.joinable()) {
        asyncThread.join();
    }

    if (syncThread.joinable()) {
        syncThread.join();
    }
}

void MessageEndpointServer::setAsyncLatch()
{
    asyncLatch = std::make_unique<faabric::util::Latch>(2);
}

void MessageEndpointServer::awaitAsyncLatch()
{
    SPDLOG_TRACE("Waiting on async latch for port {}", asyncPort);
    asyncLatch->wait();

    SPDLOG_TRACE("Finished async latch for port {}", asyncPort);
    asyncLatch = nullptr;
}
}
