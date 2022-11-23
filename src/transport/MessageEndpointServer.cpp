#include <faabric/transport/Message.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/common.h>
#include <faabric/util/bytes.h>
#include <faabric/util/latch.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/network.h>

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <memory>

namespace faabric::transport {

MessageEndpointServerHandler::MessageEndpointServerHandler(
  MessageEndpointServer* serverIn,
  bool asyncIn,
  const std::string& inprocLabelIn,
  int nThreadsIn)
  : server(serverIn)
  , async(asyncIn)
  , inprocLabel(inprocLabelIn)
  , nThreads(nThreadsIn)
{
}

void MessageEndpointServerHandler::start(int timeoutMs)
{
    // For both sync and async, we want to fan out the messages to multiple
    // worker threads.
    // Unlike zeromq, using nng_context objects we can have multiple
    // load-balanced threads receiving from the same req-rep/push-pull socket.

    // Latch to make sure we can control the order of the setup
    std::shared_ptr<faabric::util::Latch> startupLatch =
      faabric::util::Latch::create(nThreads + 1);

    SPDLOG_TRACE("Setting up endpoint server {} with {} worker threads",
                 inprocLabel,
                 nThreads);

    int port = async ? server->asyncPort : server->syncPort;

    // Connect the relevant fan-in/ out sockets (these will run until
    // they receive a terminate message)
    if (async) {
        // Set up push/ pull pair
        fan = std::make_shared<AsyncFanMessageEndpoint>(port);
    } else {
        // Set up router/ dealer
        fan = std::make_shared<SyncFanMessageEndpoint>(port);
    }

    SPDLOG_TRACE("Endpoint server {} receiver set up", port);

    for (int i = 0; i < nThreads; i++) {
        workerThreads.emplace_back([this, i, timeoutMs, startupLatch] {
            // Here we want to isolate all ZeroMQ stuff in its own
            // context, so we can do things after it's been destroyed
            {
                MessageContext endpointContext = fan->attachFanOut();

                // Notify receiver that this worker is set up

                startupLatch->wait();

                while (true) {
                    // Receive the message
                    Message body = fan->recv(endpointContext);

                    // Shut down if necessary
                    if (body.getResponseCode() == MessageResponseCode::TERM) {
                        break;
                    }

                    // On timeout we listen again
                    if (body.getResponseCode() ==
                        MessageResponseCode::TIMEOUT) {
                        continue;
                    }

                    // Catch-all for other forms of unsuccessful message
                    if (body.getResponseCode() !=
                        MessageResponseCode::SUCCESS) {
                        SPDLOG_ERROR("Unsuccessful message to server {}: {}",
                                     inprocLabel,
                                     static_cast<int>(body.getResponseCode()));

                        throw std::runtime_error(
                          "Unsuccessful message to server");
                    }

                    if (async) {
                        // Server-specific async handling
                        server->doAsyncRecv(body);
                    } else {
                        // Server-specific sync handling
                        std::unique_ptr<google::protobuf::Message> resp =
                          server->doSyncRecv(body);

                        std::string buffer;
                        if (!resp->SerializeToString(&buffer)) {
                            throw std::runtime_error(
                              "Error serialising message");
                        }

                        // Return the response
                        fan->sendResponse(
                          endpointContext,
                          NO_HEADER,
                          reinterpret_cast<uint8_t*>(buffer.data()),
                          buffer.size());
                    }

                    // Wait on the request latch if necessary
                    auto requestLatch = std::atomic_load_explicit(
                      &server->requestLatch, std::memory_order_acquire);
                    if (requestLatch != nullptr) {
                        SPDLOG_TRACE("Server thread waiting on worker latch");
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
                SPDLOG_TRACE("Server thread {} waiting on shutdown latch", i);
                shutdownLatch->wait();
            }
        });
    }

    // Wait for the workers and receiver to be set up
    startupLatch->wait();

    SPDLOG_TRACE("Endpoint server {} finished setup with {} worker threads",
                 inprocLabel,
                 nThreads);
}

void MessageEndpointServerHandler::join()
{
    // Shut down the sockets to gracefully terminate all worker threads.
    if (fan != nullptr) {
        fan->stop();
        fan = nullptr;
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
void MessageEndpointServer::start(int timeoutMs)
{
    started = true;

    asyncHandler.start(timeoutMs);
    syncHandler.start(timeoutMs);

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

        asyncShutdownSender.send(
          SHUTDOWN_HEADER, shutdownPayload.data(), shutdownPayload.size());

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

        syncShutdownSender.sendAwaitResponse(
          SHUTDOWN_HEADER, shutdownPayload.data(), shutdownPayload.size());

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
