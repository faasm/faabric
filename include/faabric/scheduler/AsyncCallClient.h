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

  private:
    const std::string host;

    grpc::CompletionQueue cq;
    std::unique_ptr<faabbric::AsyncRPCService::Stub> stub;

    struct AsyncClientCall {
        faabric::FunctionStatusResponse response;

        grpc::ClientContext context;
        grpc::Status status;

        std::unique_ptr<grpc::ClientAsyncResponseReader<FunctionStatusResponse>> response;
    };
};
}
