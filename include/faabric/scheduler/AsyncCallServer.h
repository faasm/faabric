#pragma once

#include <faabric/util/queue.h>

#include <faabric/proto/faabric.grpc.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/RPCServer.h>

#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>

#include <grpcpp/grpcpp.h>

using namespace grpc;

namespace faabric::scheduler {
class AsyncCallServer final : public rpc::RPCServer
{
  public:
    AsyncCallServer();

  protected:
    void doStart(const std::string& serverAddr) override;

    void doStop() override;

  private:
    // std::unique_ptr<faabric::util::Queue<std::shared_ptr<faabric::MPIMessage>>>
    // mpiQueue;

    std::unique_ptr<grpc::ServerCompletionQueue> cq;
    AsyncRPCService::AsyncService service;

    // Sub-class used to encapsulate the logic to serve a type of async request
    class CallData
    {
      public:
        CallData(AsyncRPCService::AsyncService* service,
                 grpc::ServerCompletionQueue* cq);

        void doRpc();

      private:
        AsyncRPCService::AsyncService* service;
        grpc::ServerCompletionQueue* cq;
        grpc::ServerContext ctx;

        // Message and response to the client
        faabric::MPIMessage msg;
        faabric::FunctionStatusResponse response;

        // Transport to answer the client
        grpc::ServerAsyncResponseWriter<faabric::FunctionStatusResponse>
          responder;

        // State machine for the serving process
        enum CallStatus
        {
            CREATE,
            PROCESS,
            FINISH
        };
        CallStatus status;
    };

    void handleRpcs();
};
}
