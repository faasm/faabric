#pragma once

#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/support/channel_arguments.h>

#include <faabric/proto/faabric.grpc.pb.h>
#include <faabric/proto/faabric.pb.h>

using namespace grpc;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

namespace faabric::scheduler {

// -----------------------------------
// gRPC client
// -----------------------------------
class AsyncCallClient
{
  public:
    explicit AsyncCallClient(const std::string& hostIn);

    void sendMpiMessage(const std::shared_ptr<faabric::MPIMessage> msg);

    void AsyncCompleteRpc();

  private:
    const std::string host;

    grpc::CompletionQueue cq;
    std::shared_ptr<Channel> channel;
    std::unique_ptr<faabric::AsyncRPCService::Stub> stub;

    // Wrapper around an individual async call
    struct AsyncCall
    {
        faabric::FunctionStatusResponse response;

        grpc::ClientContext context;
        grpc::Status status;

        std::unique_ptr<grpc::ClientAsyncResponseReader<FunctionStatusResponse>>
          response_reader;
    };
};
}
