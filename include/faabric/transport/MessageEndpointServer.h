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

class MessageEndpointServerThread
{
  public:
    MessageEndpointServerThread(MessageEndpointServer* serverIn, bool asyncIn);

    void start(std::shared_ptr<faabric::util::Latch> latch);

    void join();

  private:
    MessageEndpointServer* server;
    bool async = false;

    std::thread backgroundThread;
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
    virtual void doAsyncRecv(faabric::transport::Message& header,
                             faabric::transport::Message& body) = 0;

    virtual std::unique_ptr<google::protobuf::Message> doSyncRecv(
      faabric::transport::Message& header,
      faabric::transport::Message& body) = 0;

    void sendSyncResponse(google::protobuf::Message* resp);

  private:
    friend class MessageEndpointServerThread;

    const int asyncPort;
    const int syncPort;

    MessageEndpointServerThread asyncThread;
    MessageEndpointServerThread syncThread;

    AsyncSendMessageEndpoint asyncShutdownSender;
    SyncSendMessageEndpoint syncShutdownSender;

    std::shared_ptr<faabric::util::Latch> asyncLatch;
};
}
