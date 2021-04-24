#pragma once

#include <faabric/proto/faabric.grpc.pb.h>
#include <faabric/proto/faabric.pb.h>

#include <grpcpp/grpcpp.h>

#include <thread>

namespace faabric::scheduler {

// -----------------------------------
// gRPC client
// -----------------------------------
class AsyncCallClient
{
  public:
    explicit AsyncCallClient(const std::string& hostIn);

    ~AsyncCallClient();

    void doShutdown();

    void sendMpiMessage(const std::shared_ptr<faabric::MPIMessage> msg);

    void AsyncCompleteRpc();

    void startResponseReaderThread();

  private:
    const std::string host;

    grpc::CompletionQueue cq;
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<faabric::AsyncRPCService::Stub> stub;
    std::thread responseThread;

    // Wrapper around an individual async call
    struct AsyncCall
    {
        faabric::FunctionStatusResponse response;

        grpc::ClientContext context;
        grpc::Status status;

        std::unique_ptr<grpc::ClientAsyncResponseReader<FunctionStatusResponse>>
          responseReader;
    };
};
}
