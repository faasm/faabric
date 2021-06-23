#pragma once

#include <google/protobuf/message.h>

#include <faabric/transport/Message.h>
#include <faabric/util/exception.h>

#include <thread>
#include <zmq.hpp>

// Defined in libzmq/include/zmq.h
#define ZMQ_ETERM ETERM

#define ANY_HOST "0.0.0.0"

// These timeouts should be long enough to permit sending and receiving large
// messages, but short enough not to hang around when something has gone wrong.
#define DEFAULT_RECV_TIMEOUT_MS 20000
#define DEFAULT_SEND_TIMEOUT_MS 20000

// How long undelivered messages will hang around when the socket is closed,
// which also determines how long the context will hang for when closing if
// things haven't yet completed (usually only when there's an error).
#define LINGER_MS 1000

namespace faabric::transport {

/* Wrapper arround zmq::socket_t
 *
 * Thread-unsafe socket-like object. MUST be open-ed and close-ed from the
 * _same_ thread. For a proto://host:pair triple, one socket may bind, and all
 * the rest must connect. Order does not matter. Sockets either send (PUSH)
 * or recv (PULL) data.
 */
class MessageEndpoint
{
  public:
    MessageEndpoint(zmq::socket_type socketTypeIn,
                    const std::string& hostIn,
                    int portIn,
                    int timeoutMsIn);

    // Delete assignment and copy-constructor as we need to be very careful with
    // socping and same-thread instantiation
    MessageEndpoint& operator=(const MessageEndpoint&) = delete;

    MessageEndpoint(const MessageEndpoint& ctx) = delete;

    std::string getHost();

    int getPort();

  protected:
    const zmq::socket_type socketType;
    const std::string host;
    const int port;
    const std::string address;
    const int timeoutMs;
    const std::thread::id tid;
    const int id;

    zmq::socket_t socket;

    void validateTimeout(int value);
};

/* Send and Recv Message Endpoints */

class SendMessageEndpoint : public MessageEndpoint
{
  public:
    SendMessageEndpoint(const std::string& hostIn,
                        int portIn,
                        int timeoutMs = DEFAULT_SEND_TIMEOUT_MS);

    void send(uint8_t* serialisedMsg, size_t msgSize, bool more = false);

    Message awaitResponse();
};

class RecvMessageEndpoint : public MessageEndpoint
{
  public:
    RecvMessageEndpoint(int portIn, int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    Message recv(int size = 0);

    /* Send response to the client
     *
     * Send a one-off response to a client identified by host:port pair.
     * Together with a blocking recv at the client side, this
     * method can be used to achieve synchronous client-server communication.
     */
    void sendResponse(uint8_t* data, int size, const std::string& returnHost);

  private:
    Message recvBuffer(int size);

    Message recvNoBuffer();
};

class MessageTimeoutException : public faabric::util::FaabricException
{
  public:
    explicit MessageTimeoutException(std::string message)
      : FaabricException(std::move(message))
    {}
};
}
