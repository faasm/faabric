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
// Mocking
// -----------------------------------
std::vector<std::pair<std::string, faabric::Message>> getFunctionCalls();

std::vector<std::pair<std::string, faabric::Message>> getFlushCalls();

std::vector<
  std::pair<std::string, std::shared_ptr<faabric::BatchExecuteRequest>>>
getBatchRequests();

std::vector<std::pair<std::string, faabric::MPIMessage>> getMPIMessages();

std::vector<std::pair<std::string, faabric::ResourceRequest>>
getResourceRequests();

std::vector<std::pair<std::string, faabric::UnregisterRequest>>
getUnregisterRequests();

std::vector<std::pair<std::string, faabric::ThreadResultRequest>>
getThreadResults();

void queueResourceResponse(const std::string& host,
                           faabric::HostResources& res);

void clearMockRequests();

// -----------------------------------
// gRPC client
// -----------------------------------
class FunctionCallClient
{
  public:
    explicit FunctionCallClient(const std::string& hostIn);

    const std::string host;

    std::shared_ptr<Channel> channel;
    std::unique_ptr<faabric::FunctionRPCService::Stub> stub;

    void sendFlush();

    void sendMPIMessage(const std::shared_ptr<faabric::MPIMessage> msg);

    faabric::HostResources getResources(const faabric::ResourceRequest& req);

    void executeFunctions(
      const std::shared_ptr<faabric::BatchExecuteRequest> req);

    void unregister(const faabric::UnregisterRequest& req);

    void setThreadResult(const faabric::ThreadResultRequest& req);
};
}
