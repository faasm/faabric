#pragma once

#include <faabric/scheduler/Scheduler.h>

#include <faabric/proto/faabric.grpc.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/RPCServer.h>

using namespace grpc;

namespace faabric::scheduler {
class AsyncCallServer final
  : public rpc::RPCServer
  , public faabric::AsyncRPCService::Service
{
  public:
    AsyncCallServer();

    Status MPIMsg(ServerContext* context,
                   const faabric::MPIMessage* request,
                   faabric::FunctionStatusResponse* response) override;

  protected:
    void doStart(const std::string& serverAddr) override;

    void doStop(const std::string& serverAddr) override;

  private:
    Scheduler& scheduler;
    std::unique_ptr<faabric::util::Queue<std::shared_ptr<faabric::MPIMessage>>> mpiQueue;

    std::unique_ptr<grpc::ServerCompletionQueue> cq;
    AsyncRPCService::AsyncService service;

    // Sub-class used to encapsulate the logic to serve a type of async request
    class CallData {
      public:
        CallData(AsyncRPCService::Service* service, 
                 grpc::ServerCompletionQueue* cq);

        void doRpc();

      private:
        AsyncRPCService::AsyncService* service;
        grpc::ServerCompletionQueue* cq;
        grpc::ServerContext* ctx;

        // Message and response to the client
        faabric::MPIMessage msg;
        faabric::FunctionStatusResponse response;

        // Transport to answer the client
        grpc::ServerAsyncResponseWriter<FunctionStatusResponse> responder;

        // State machine for the serving process
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status;
    };

    void handleRpcs();
};
}
