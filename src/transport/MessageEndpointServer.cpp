#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/common.h>
#include <faabric/util/latch.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>

#include <csignal>
#include <cstdlib>

namespace faabric::transport {

static const std::vector<uint8_t> shutdownHeader = { 0, 0, 1, 1 };

MessageEndpointServerThread::MessageEndpointServerThread(
  MessageEndpointServer* serverIn,
  bool asyncIn)
  : server(serverIn)
  , async(asyncIn)
{}

void MessageEndpointServerThread::start(
  std::shared_ptr<faabric::util::Latch> latch)
{
    backgroundThread = std::thread([this, latch] {
        std::unique_ptr<RecvMessageEndpoint> endpoint = nullptr;
        int port = -1;

        if (async) {
            port = server->asyncPort;
            endpoint = std::make_unique<AsyncRecvMessageEndpoint>(port);
        } else {
            port = server->syncPort;
            endpoint = std::make_unique<SyncRecvMessageEndpoint>(port);
        }

        latch->wait();

        while (true) {
            bool headerReceived = false;
            bool bodyReceived = false;
            try {
                // Receive header and body
                Message headerMessage = endpoint->recv();
                headerReceived = true;

                if (headerMessage.size() == shutdownHeader.size()) {
                    if (headerMessage.dataCopy() == shutdownHeader) {
                        SPDLOG_TRACE("Server on {} received shutdown message",
                                     port);
                        break;
                    }
                }

                if (!headerMessage.more()) {
                    throw std::runtime_error(
                      "Header sent without SNDMORE flag");
                }

                Message body = endpoint->recv();
                if (body.more()) {
                    throw std::runtime_error("Body sent with SNDMORE flag");
                }
                bodyReceived = true;

                assert(headerMessage.size() == sizeof(uint8_t));
                uint8_t header = static_cast<uint8_t>(*headerMessage.data());

                if (async) {
                    // Server-specific async handling
                    server->doAsyncRecv(header, body.udata(), body.size());
                } else {
                    // Server-specific sync handling
                    std::unique_ptr<google::protobuf::Message> resp =
                      server->doSyncRecv(header, body.udata(), body.size());
                    size_t respSize = resp->ByteSizeLong();

                    uint8_t buffer[respSize];
                    if (!resp->SerializeToArray(buffer, respSize)) {
                        throw std::runtime_error("Error serialising message");
                    }

                    // Return the response
                    static_cast<SyncRecvMessageEndpoint*>(endpoint.get())
                      ->sendResponse(buffer, respSize);
                }
            } catch (MessageTimeoutException& ex) {
                // If we don't get a header in the timeout, we're ok to just
                // loop round and try again
                if (!headerReceived) {
                    SPDLOG_TRACE("Server on port {}, looping after no message",
                                 port);
                    continue;
                }

                if (headerReceived && !bodyReceived) {
                    SPDLOG_ERROR(
                      "Server on port {}, got header, timed out on body", port);
                    throw;
                }
            }

            // Wait on the async latch if necessary
            if (server->asyncLatch != nullptr) {
                SPDLOG_TRACE("Server thread waiting on async latch");
                server->asyncLatch->wait();
            }

            headerReceived = false;
            bodyReceived = false;
        }
    });
}

void MessageEndpointServerThread::join()
{
    if (backgroundThread.joinable()) {
        backgroundThread.join();
    }
}

MessageEndpointServer::MessageEndpointServer(int asyncPortIn, int syncPortIn)
  : asyncPort(asyncPortIn)
  , syncPort(syncPortIn)
  , asyncThread(this, true)
  , syncThread(this, false)
  , asyncShutdownSender(LOCALHOST, asyncPort)
  , syncShutdownSender(LOCALHOST, syncPort)
{}

void MessageEndpointServer::start()
{
    // This latch means that callers can guarantee that when this function
    // completes, both sockets will have been opened (and hence the server is
    // ready to use).
    auto startLatch = faabric::util::Latch::create(3);

    asyncThread.start(startLatch);
    syncThread.start(startLatch);

    startLatch->wait();
}

void MessageEndpointServer::stop()
{
    // Send shutdown messages
    SPDLOG_TRACE(
      "Server sending shutdown messages to ports {} {}", asyncPort, syncPort);

    asyncShutdownSender.send(shutdownHeader.data(), shutdownHeader.size());

    syncShutdownSender.sendRaw(shutdownHeader.data(), shutdownHeader.size());

    // Join the threads
    asyncThread.join();
    syncThread.join();
}

void MessageEndpointServer::setAsyncLatch()
{
    asyncLatch = faabric::util::Latch::create(2);
}

void MessageEndpointServer::awaitAsyncLatch()
{
    SPDLOG_TRACE("Waiting on async latch for port {}", asyncPort);
    asyncLatch->wait();

    SPDLOG_TRACE("Finished async latch for port {}", asyncPort);
    asyncLatch = nullptr;
}
}
