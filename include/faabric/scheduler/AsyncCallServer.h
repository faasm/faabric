#pragma once

#include <faabric/proto/faabric.grpc.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/RPCServer.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/util/locks.h>
#include <faabric/util/queue.h>

#include <grpcpp/grpcpp.h>

using namespace grpc;

namespace faabric::scheduler {
class AsyncCallServer final : public rpc::RPCServer
{
  public:
    AsyncCallServer();

    ~AsyncCallServer();

  protected:
    void doStart(const std::string& serverAddr) override;

    void doStop() override;

  private:
    const unsigned int numServerThreads;
    const unsigned int numServerContexts;

    std::unique_ptr<grpc::ServerCompletionQueue> cq;
    faabric::AsyncRPCService::AsyncService service;
    bool isShutdown;

    // Context for a single outstanding RPC
    struct RpcContext
    {
        std::unique_ptr<grpc::ServerContext> serverContext;
        // Transport to write the response
        std::unique_ptr<
          grpc::ServerAsyncResponseWriter<faabric::FunctionStatusResponse>>
          responseWriter;
        // Inbound type
        faabric::MPIMessage msg;
        enum
        {
            READY,
            DONE
        } state;
    };

    // Multi-threaded server support
    std::vector<std::thread> serverThreads;
    std::vector<RpcContext> serverRpcContexts;
    std::mutex serverMutex;

    void handleRpcs();

    void refreshContext(int i);
};
}
