#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/Message.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/util/latch.h>

#include <thread>

namespace faabric::transport {

// Each server has two underlying sockets, one for synchronous communication and
// one for asynchronous. Each is run inside its own background thread.
class MessageEndpointServer;

class MessageEndpointServerHandler
{
  public:
    MessageEndpointServerHandler(MessageEndpointServer* serverIn, bool asyncIn);

    void start(std::shared_ptr<faabric::util::Latch> latch);

    void join();

  private:
    MessageEndpointServer* server;
    bool async = false;

    std::thread receiverThread;

    std::vector<std::thread> workerThreads;
};

class MessageEndpointServer
{
  public:
    MessageEndpointServer(int asyncPortIn, int syncPortIn);

    virtual void start();

    virtual void stop();

    void setAsyncLatch();

    void awaitAsyncLatch();

  protected:
    virtual void doAsyncRecv(int header,
                             const uint8_t* buffer,
                             size_t bufferSize) = 0;

    virtual std::unique_ptr<google::protobuf::Message>
    doSyncRecv(int header, const uint8_t* buffer, size_t bufferSize) = 0;

  private:
    friend class MessageEndpointServerHandler;

    const int asyncPort;
    const int syncPort;

    MessageEndpointServerHandler asyncHandler;
    MessageEndpointServerHandler syncHandler;

    AsyncSendMessageEndpoint asyncShutdownSender;
    SyncSendMessageEndpoint syncShutdownSender;

    std::shared_ptr<faabric::util::Latch> asyncLatch;
};
}
