#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/common.h>
#include <faabric/util/barrier.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>

#include <csignal>
#include <cstdlib>

namespace faabric::transport {
MessageEndpointServer::MessageEndpointServer(int asyncPortIn, int syncPortIn)
  : asyncPort(asyncPortIn)
  , syncPort(syncPortIn)
{}

void MessageEndpointServer::start()
{
    // This barrier means that callers can guarantee that when this function
    // completes, both sockets will have been opened (and hence the server is
    // ready to use).
    faabric::util::Barrier startBarrier(3);

    asyncThread = std::thread([this, &startBarrier] {
        AsyncRecvMessageEndpoint endpoint(asyncPort);
        startBarrier.wait();

        // Loop until we receive a shutdown message
        while (true) {
            // Receive header and body
            Message header = endpoint.recv();

            // Detect shutdown condition
            if (header.size() == sizeof(uint8_t) && !header.more()) {
                SPDLOG_TRACE("Async server socket received shutdown message");
                break;
            }

            // Check the header was sent with ZMQ_SNDMORE flag
            if (!header.more()) {
                throw std::runtime_error("Header sent without SNDMORE flag");
            }

            // Check that there are no more messages to receive
            Message body = endpoint.recv();
            if (body.more()) {
                throw std::runtime_error("Body sent with SNDMORE flag");
            }
            assert(body.udata() != nullptr);

            // Server-specific message handling
            doAsyncRecv(header, body);
        }
    });

    syncThread = std::thread([this, &startBarrier] {
        SyncRecvMessageEndpoint endpoint(syncPort);
        startBarrier.wait();

        // Loop until we receive a shutdown message
        while (true) {
            // Receive header and body
            Message header = endpoint.recv();

            // Detect shutdown condition
            if (header.size() == sizeof(uint8_t) && !header.more()) {
                SPDLOG_TRACE("Sync server socket received shutdown message");
                break;
            }

            // Check the header was sent with ZMQ_SNDMORE flag
            if (!header.more()) {
                throw std::runtime_error("Header sent without SNDMORE flag");
            }

            // Check that there are no more messages to receive
            Message body = endpoint.recv();
            if (body.more()) {
                throw std::runtime_error("Body sent with SNDMORE flag");
            }

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

    startBarrier.wait();
}

void MessageEndpointServer::stop()
{
    SPDLOG_TRACE(
      "Sending sync shutdown message locally to {}:{}", LOCALHOST, syncPort);

    SyncSendMessageEndpoint syncSender(LOCALHOST, syncPort);
    syncSender.sendShutdown();

    SPDLOG_TRACE(
      "Sending async shutdown message locally to {}:{}", LOCALHOST, asyncPort);

    AsyncSendMessageEndpoint asyncSender(LOCALHOST, asyncPort);
    asyncSender.sendShutdown();

    // Join the threads
    if (asyncThread.joinable()) {
        asyncThread.join();
    }

    if (syncThread.joinable()) {
        syncThread.join();
    }
}

}
