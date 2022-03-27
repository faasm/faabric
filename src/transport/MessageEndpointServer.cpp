#include "faabric/transport/Message.h"
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/common.h>
#include <faabric/util/latch.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/network.h>

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <memory>

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

        if (async) {
            // Set up push/ pull pair
            asyncFanIn = std::make_unique<AsyncFanInMessageEndpoint>(port);
            asyncFanOut =
              std::make_unique<AsyncFanOutMessageEndpoint>(inprocLabel);
        } else {
            // Set up router/ dealer
            syncFanIn = std::make_unique<SyncFanInMessageEndpoint>(port);
            syncFanOut =
              std::make_unique<SyncFanOutMessageEndpoint>(inprocLabel);
        }

        // Launch worker threads
        for (int i = 0; i < nThreads; i++) {
            workerThreads.emplace_back([this, i] {
                // Here we want to isolate all ZeroMQ stuff in its own
                // context, so we can do things after it's been destroyed
                {
                    std::unique_ptr<RecvMessageEndpoint> endpoint = nullptr;

                    if (async) {
                        // Async workers have a PULL socket
                        endpoint = std::make_unique<AsyncRecvMessageEndpoint>(
                          inprocLabel);
                    } else {
                        // Sync workers have an in-proc REP socket
                        endpoint = std::make_unique<SyncRecvMessageEndpoint>(
                          inprocLabel);
                    }

                    while (true) {
                        // Receive header and body
                        Message headerMessage = endpoint->recv();
                        if (headerMessage.getFailCode() ==
                            MessageFailCode::TIMEOUT) {
                            SPDLOG_TRACE(
                              "Server on {}, looping after no message",
                              endpoint->getAddress());
                            continue;
                        }

                        if (headerMessage.size() == shutdownHeader.size()) {
                            if (headerMessage.dataCopy() == shutdownHeader) {
                                SPDLOG_TRACE(
                                  "Server thread {} on {} got shutdown message",
                                  i,
                                  endpoint->getAddress());

                                // Send an empty response if in sync mode
                                // (otherwise upstream socket will hang)
                                if (!async) {
                                    std::vector<uint8_t> empty(4, 0);
                                    static_cast<SyncRecvMessageEndpoint*>(
                                      endpoint.get())
                                      ->sendResponse(empty.data(),
                                                     empty.size());
                                }

                                break;
                            }
                        }

                        if (!headerMessage.more()) {
                            throw std::runtime_error(
                              "Header sent without SNDMORE flag");
                        }

                        Message body = endpoint->recv();
                        if (body.getFailCode() != MessageFailCode::SUCCESS) {
                            SPDLOG_ERROR("Server on port {}, got header, error "
                                         "on body: {}",
                                         endpoint->getAddress(),
                                         body.getFailCode());
                            throw MessageTimeoutException(
                              "Server, got header, error on body");
                        }

                        if (body.more()) {
                            throw std::runtime_error(
                              "Body sent with SNDMORE flag");
                        }

                        assert(headerMessage.size() == sizeof(uint8_t));
                        uint8_t header =
                          static_cast<uint8_t>(*headerMessage.data());

                        if (async) {
                            // Server-specific async handling
                            server->doAsyncRecv(header, std::move(body));
                        } else {
                            // Server-specific sync handling
                            std::unique_ptr<google::protobuf::Message> resp =
                              server->doSyncRecv(header, std::move(body));

                            size_t respSize = resp->ByteSizeLong();

                            uint8_t buffer[respSize];
                            if (!resp->SerializeToArray(buffer, respSize)) {
                                throw std::runtime_error(
                                  "Error serialising message");
                            }

                            // Return the response
                            static_cast<SyncRecvMessageEndpoint*>(
                              endpoint.get())
                              ->sendResponse(buffer, respSize);
                        }

                        // Wait on the request latch if necessary
                        auto requestLatch = std::atomic_load_explicit(
                          &server->requestLatch, std::memory_order_acquire);
                        if (requestLatch != nullptr) {
                            SPDLOG_TRACE(
                              "Server thread waiting on worker latch");
                            requestLatch->wait();
                        }
                    }
                }

                // Perform the tidy-up
                server->onWorkerStop();

                // Just before the thread dies, check if there's something
                // waiting on the shutdown latch
                auto shutdownLatch = std::atomic_load_explicit(
                  &server->shutdownLatch, std::memory_order_acquire);
                if (shutdownLatch != nullptr) {
                    SPDLOG_TRACE("Server thread {} waiting on shutdown latch",
                                 i);
                    shutdownLatch->wait();
                }
            });
        }

        // Wait on the start-up latch passed in by the caller. This means that
        // once the latch is freed, this handler is just about to start its
        // proxy, so a short sleep should mean things are ready to go.
        latch->wait();

        // Connect the relevant fan-in/ out sockets (these will run until
        // they receive a terminate message)
        if (async) {
            asyncFanIn->attachFanOut(asyncFanOut->socket);
        } else {
            syncFanIn->attachFanOut(syncFanOut->socket);
        }
    });
}

void MessageEndpointServerHandler::join()
{
    // Note that we have to kill any running proxies before anything else
    // https://github.com/zeromq/cppzmq/issues/478
    if (syncFanIn != nullptr) {
        syncFanIn->stop();
    }

    if (asyncFanIn != nullptr) {
        asyncFanIn->stop();
    }

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

/**
 * We need to guarantee to callers of this function, that when it returns, the
 * server will be ready to use.
 */
void MessageEndpointServer::start()
{
    started = true;

    // This latch allows use to block on the handlers until _just_ before they
    // start their proxies.
    auto startLatch = faabric::util::Latch::create(3);

    asyncHandler.start(startLatch);
    syncHandler.start(startLatch);

    startLatch->wait();

    // Unfortunately we can't know precisely when the proxies have started,
    // hence have to add a sleep.
    SLEEP_MS(500);
}

void MessageEndpointServer::stop()
{
    if (!started) {
        SPDLOG_DEBUG("Not stopping server on {}, not started", syncPort);
        return;
    }

    // Here we send shutdown messages to each worker in turn, however, because
    // they're all connected on the same inproc port, we have to wait until each
    // one has shut down fully (i.e. the zmq socket has gone out of scope),
    // before sending the next shutdown message.
    // If we don't do this, zmq will direct messages to sockets that are in the
    // process of shutting down and cause errors.
    // To ensure each socket has closed, we use a latch with two slots, where
    // this thread takes one of the slots, and the worker thread takes the other
    // once it's finished shutting down.
    for (int i = 0; i < nThreads; i++) {
        SPDLOG_TRACE("Sending async shutdown message {}/{} to port {}",
                     i + 1,
                     nThreads,
                     asyncPort);

        std::atomic_store_explicit(&shutdownLatch,
                                   faabric::util::Latch::create(2),
                                   std::memory_order_release);

        asyncShutdownSender.send(shutdownHeader.data(), shutdownHeader.size());

        std::atomic_load_explicit(&shutdownLatch, std::memory_order_acquire)
          ->wait();
        std::atomic_store_explicit(
          &shutdownLatch,
          std::shared_ptr<faabric::util::Latch>(nullptr),
          std::memory_order_release);
    }

    for (int i = 0; i < nThreads; i++) {
        SPDLOG_TRACE("Sending sync shutdown message {}/{} to port {}",
                     i + 1,
                     nThreads,
                     syncPort);

        std::atomic_store_explicit(&shutdownLatch,
                                   faabric::util::Latch::create(2),
                                   std::memory_order_release);

        syncShutdownSender.sendAwaitResponse(shutdownHeader.data(),
                                             shutdownHeader.size());

        std::atomic_load_explicit(&shutdownLatch, std::memory_order_acquire)
          ->wait();
        std::atomic_store_explicit(
          &shutdownLatch,
          std::shared_ptr<faabric::util::Latch>(nullptr),
          std::memory_order_release);
    }

    // Join the handlers
    asyncHandler.join();
    syncHandler.join();

    started = false;
}

void MessageEndpointServer::onWorkerStop()
{
    // Nothing to do by default
}

void MessageEndpointServer::setRequestLatch()
{
    std::atomic_store_explicit(&requestLatch,
                               faabric::util::Latch::create(2),
                               std::memory_order_release);
}

void MessageEndpointServer::awaitRequestLatch()
{
    SPDLOG_TRACE("Waiting on worker latch for port {}", asyncPort);
    std::atomic_load_explicit(&requestLatch, std::memory_order_acquire)->wait();

    SPDLOG_TRACE("Finished worker latch for port {}", asyncPort);
    std::atomic_store_explicit(&requestLatch,
                               std::shared_ptr<faabric::util::Latch>(nullptr),
                               std::memory_order_release);
}

int MessageEndpointServer::getNThreads()
{
    return nThreads;
}
}
