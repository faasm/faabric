#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/Message.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/util/latch.h>

#include <thread>

#define DEFAULT_MESSAGE_SERVER_THREADS 4

namespace faabric::transport {

// Each server has two underlying sockets, one for synchronous communication and
// one for asynchronous. Each is run inside its own background thread.
class MessageEndpointServer;

class MessageEndpointServerHandler
{
  public:
    MessageEndpointServerHandler(MessageEndpointServer* serverIn,
                                 bool asyncIn,
                                 const std::string& inprocLabelIn,
                                 int nThreadsIn);

    void start(int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    void join();

  private:
    MessageEndpointServer* server;
    bool async = false;
    const std::string inprocLabel;
    int nThreads;

    std::jthread receiverThread;

    std::vector<std::jthread> workerThreads;

    std::shared_ptr<FanMessageEndpoint> fan = nullptr;
};

class MessageEndpointServer
{
  public:
    MessageEndpointServer(int asyncPortIn,
                          int syncPortIn,
                          const std::string& inprocLabelIn,
                          int nThreadsIn);

    virtual void start(int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    virtual void stop();

    virtual void onWorkerStop();

    void setRequestLatch();

    void awaitRequestLatch();

    int getNThreads();

  protected:
    virtual void doAsyncRecv(transport::Message& message) = 0;

    virtual std::unique_ptr<google::protobuf::Message> doSyncRecv(
      transport::Message& message) = 0;

  private:
    friend class MessageEndpointServerHandler;

    const int asyncPort;
    const int syncPort;
    const std::string inprocLabel;
    const int nThreads;

    MessageEndpointServerHandler asyncHandler;
    MessageEndpointServerHandler syncHandler;

    std::shared_ptr<faabric::util::Latch> requestLatch;

    bool started = false;
};
}
