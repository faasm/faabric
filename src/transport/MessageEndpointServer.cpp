#include "faabric/transport/MessageEndpoint.h"
#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/common.h>
#include <faabric/util/latch.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>

#include <csignal>
#include <cstdlib>

namespace faabric::transport {

static const std::vector<uint8_t> shutdownHeader = { 0, 0, 1, 1 };

MessageEndpointServerHandler::MessageEndpointServerHandler(
  MessageEndpointServer* serverIn,
  bool asyncIn,
  const std::string& inprocLabelIn,
  int nThreadsIn)
  : server(serverIn)
  , async(asyncIn)
  , inprocLabel(inprocLabelIn)
  , nThreads(nThreadsIn)
{}

void MessageEndpointServerHandler::start(
  std::shared_ptr<faabric::util::Latch> latch)
{
    // For both sync and async, we want to fan out the messages to multiple
    // worker threads.
    // For sync, we use the router/ dealer pattern:
    // https://zguide.zeromq.org/docs/chapter2/#Multithreading-with-ZeroMQ
    // For push/ pull we receive on a pull socket, then proxy with another push
    // to multiple downstream pull sockets
    // In both cases, the downstream fan-out is done over inproc sockets.
    receiverThread = std::thread([this, latch] {
        int port = async ? server->asyncPort : server->syncPort;

        // Sync: router and dealer
        std::unique_ptr<SyncFanInMessageEndpoint> syncFanIn = nullptr;
        std::unique_ptr<SyncFanOutMessageEndpoint> syncFanOut = nullptr;

        // Async: pull/ push pair
        std::unique_ptr<AsyncFanInMessageEndpoint> asyncFanIn = nullptr;
        std::unique_ptr<AsyncFanOutMessageEndpoint> asyncFanOut = nullptr;

        if (async) {
            asyncFanIn = std::make_unique<AsyncFanInMessageEndpoint>(port);
            asyncFanOut =
              std::make_unique<AsyncFanOutMessageEndpoint>(inprocLabel);
        } else {
            // Set up router/ dealer
            syncFanIn = std::make_unique<SyncFanInMessageEndpoint>(port);
            syncFanOut =
              std::make_unique<SyncFanOutMessageEndpoint>(inprocLabel);
        }

        // Lauch worker threads
        for (int i = 0; i < nThreads; i++) {
            workerThreads.emplace_back([this] {
                std::unique_ptr<RecvMessageEndpoint> endpoint = nullptr;

                if (async) {
                    // Async workers have a PULL socket
                    endpoint =
                      std::make_unique<AsyncRecvMessageEndpoint>(inprocLabel);
                } else {
                    // Sync workers have an in-proc REP socket
                    endpoint =
                      std::make_unique<SyncRecvMessageEndpoint>(inprocLabel);
                }

                while (true) {
                    // Receive header and body
                    std::optional<Message> headerMessageMaybe =
                      endpoint->recv();
                    if (!headerMessageMaybe.has_value()) {
                        SPDLOG_TRACE("Server on {}, looping after no message",
                                     endpoint->getAddress());
                        continue;
                    }

                    Message& headerMessage = headerMessageMaybe.value();

                    if (headerMessage.size() == shutdownHeader.size()) {
                        if (headerMessage.dataCopy() == shutdownHeader) {
                            SPDLOG_TRACE(
                              "Server on {} received shutdown message",
                              endpoint->getAddress());

                            // Allow things to wait on shutdown
                            if (server->workerLatch != nullptr) {
                                server->workerLatch->wait();
                            }

                            break;
                        }
                    }

                    if (!headerMessage.more()) {
                        throw std::runtime_error(
                          "Header sent without SNDMORE flag");
                    }

                    std::optional<Message> bodyMaybe = endpoint->recv();
                    if (!bodyMaybe.has_value()) {
                        SPDLOG_ERROR(
                          "Server on port {}, got header, timed out on body",
                          endpoint->getAddress());
                        throw MessageTimeoutException(
                          "Server, got header, timed out on body");
                    }

                    Message& body = bodyMaybe.value();
                    if (body.more()) {
                        throw std::runtime_error("Body sent with SNDMORE flag");
                    }

                    assert(headerMessage.size() == sizeof(uint8_t));
                    uint8_t header =
                      static_cast<uint8_t>(*headerMessage.data());

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
                            throw std::runtime_error(
                              "Error serialising message");
                        }

                        // Return the response
                        static_cast<SyncRecvMessageEndpoint*>(endpoint.get())
                          ->sendResponse(buffer, respSize);
                    }

                    // Wait on the async latch if necessary
                    if (server->workerLatch != nullptr) {
                        SPDLOG_TRACE("Server thread waiting on async latch");
                        server->workerLatch->wait();
                    }
                }
            });
        }

        // Wait on the latch
        latch->wait();

        // Connect the router and dealer if sync
        if (async) {
            asyncFanIn->attachFanOut(asyncFanOut);
        } else {
            syncFanIn->attachFanOut(syncFanOut);
        }
    });
}

void MessageEndpointServerHandler::join()
{
    // Join each worker
    for (auto& t : workerThreads) {
        if (t.joinable()) {
            t.join();
        }
    }

    // Join the receiver thread
    if (receiverThread.joinable()) {
        receiverThread.join();
    }
}

MessageEndpointServer::MessageEndpointServer(int asyncPortIn,
                                             int syncPortIn,
                                             const std::string& inprocLabelIn,
                                             int nThreadsIn)
  : asyncPort(asyncPortIn)
  , syncPort(syncPortIn)
  , inprocLabel(inprocLabelIn)
  , nThreads(nThreadsIn)
  , asyncHandler(this, true, inprocLabel + "-async", nThreadsIn)
  , syncHandler(this, false, inprocLabel, nThreadsIn)
  , asyncShutdownSender(LOCALHOST, asyncPort)
  , syncShutdownSender(LOCALHOST, syncPort)
{}

void MessageEndpointServer::start()
{
    // This latch means that callers can guarantee that when this function
    // completes, both sockets will have been opened (and hence the server is
    // ready to use).
    auto startLatch = faabric::util::Latch::create(3);

    asyncHandler.start(startLatch);
    syncHandler.start(startLatch);

    startLatch->wait();
}

void MessageEndpointServer::stop()
{
    for (int i = 0; i < nThreads; i++) {
        setWorkerLatch();
        SPDLOG_TRACE("Sending async shutdown message {}/{} to port {}",
                     i + 1,
                     nThreads,
                     asyncPort);

        asyncShutdownSender.send(shutdownHeader.data(), shutdownHeader.size());
        awaitWorkerLatch();
    }

    for (int i = 0; i < nThreads; i++) {
        setWorkerLatch();
        SPDLOG_TRACE("Sending sync shutdown message {}/{} to port {}",
                     i + 1,
                     nThreads,
                     syncPort);

        syncShutdownSender.sendRaw(shutdownHeader.data(),
                                   shutdownHeader.size());
        awaitWorkerLatch();
    }

    // Join the handlers
    asyncHandler.join();
    syncHandler.join();
}

void MessageEndpointServer::setWorkerLatch()
{
    workerLatch = faabric::util::Latch::create(2);
}

void MessageEndpointServer::awaitWorkerLatch()
{
    SPDLOG_TRACE("Waiting on async latch for port {}", asyncPort);
    workerLatch->wait();

    SPDLOG_TRACE("Finished async latch for port {}", asyncPort);
    workerLatch = nullptr;
}
}
