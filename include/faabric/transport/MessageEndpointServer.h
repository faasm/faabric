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

    void start(std::shared_ptr<faabric::util::Latch> latch);

    void join();

  private:
    MessageEndpointServer* server;
    bool async = false;
    const std::string inprocLabel;
    int nThreads;

    std::thread receiverThread;

    std::vector<std::thread> workerThreads;

    std::unique_ptr<AsyncFanInMessageEndpoint> asyncFanIn = nullptr;
    std::unique_ptr<SyncFanInMessageEndpoint> syncFanIn = nullptr;
};

class MessageEndpointServer
{
  public:
    MessageEndpointServer(int asyncPortIn,
                          int syncPortIn,
                          const std::string& inprocLabelIn,
                          int nThreadsIn);

    virtual void start();

    virtual void stop();

    void setWorkerLatch();

    void awaitWorkerLatch();

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
    const std::string inprocLabel;
    const int nThreads;

    MessageEndpointServerHandler asyncHandler;
    MessageEndpointServerHandler syncHandler;

    AsyncSendMessageEndpoint asyncShutdownSender;
    SyncSendMessageEndpoint syncShutdownSender;

    std::shared_ptr<faabric::util::Latch> workerLatch;
};
}
