#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/Message.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/util/latch.h>

#include <thread>

namespace faabric::transport {

// This server has two underlying sockets, one for synchronous communication and
// one for asynchronous.
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
    const int asyncPort;
    const int syncPort;

    std::thread asyncThread;
    std::thread syncThread;

    AsyncSendMessageEndpoint asyncShutdownSender;
    SyncSendMessageEndpoint syncShutdownSender;

    std::unique_ptr<faabric::util::Latch> asyncLatch;
};
}
