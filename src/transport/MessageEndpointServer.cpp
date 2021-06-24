#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/common.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>

#include <csignal>
#include <cstdlib>

namespace faabric::transport {
MessageEndpointServer::MessageEndpointServer(int portIn)
  : asyncPort(portIn)
  , syncPort(portIn + 1)
{}

void MessageEndpointServer::start()
{
    asyncThread = std::thread([this] {
        AsyncRecvMessageEndpoint endpoint(asyncPort);

        // Loop until we receive a shutdown message
        while (true) {
            // Receive header and body
            Message header = endpoint.recv();

            // Detect shutdown condition
            if (header.size() == 0) {
                SPDLOG_TRACE("Server received shutdown message");
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

    syncThread = std::thread([this] {
        SyncRecvMessageEndpoint endpoint(syncPort);

        // Loop until we receive a shutdown message
        while (true) {
            // Receive header and body
            Message header = endpoint.recv();

            // Detect shutdown condition
            if (header.size() == 0) {
                SPDLOG_TRACE("Server received shutdown message");
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
            std::unique_ptr<google::protobuf::Message> resp =
              doSyncRecv(header, body);
            size_t msgSize = resp->ByteSizeLong();
            {
                uint8_t sMsg[msgSize];
                if (!resp->SerializeToArray(sMsg, msgSize)) {
                    throw std::runtime_error("Error serialising message");
                }
                endpoint.sendResponse(sMsg, msgSize);
            }
        }
    });
}

void MessageEndpointServer::stop()
{
    SPDLOG_TRACE(
      "Sending sync shutdown message locally to {}:{}", LOCALHOST, syncPort);

    SyncSendMessageEndpoint syncSender(LOCALHOST, syncPort);
    syncSender.sendAwaitResponse(nullptr, 0);

    SPDLOG_TRACE(
      "Sending async shutdown message locally to {}:{}", LOCALHOST, asyncPort);

    AsyncSendMessageEndpoint asyncSender(LOCALHOST, syncPort);
    asyncSender.send(nullptr, 0);

    // Join the threads
    if (asyncThread.joinable()) {
        asyncThread.join();
    }

    if (syncThread.joinable()) {
        syncThread.join();
    }
}
}
