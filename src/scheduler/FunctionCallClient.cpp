#include "faabric/proto/faabric.pb.h"
#include <faabric/scheduler/FunctionCallClient.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <faabric/proto/macros.h>

namespace faabric::scheduler {
FunctionCallClient::FunctionCallClient(const std::string& hostIn)
  : host(hostIn)
  , channel(grpc::CreateChannel(host + ":" + std::to_string(FUNCTION_CALL_PORT),
                                grpc::InsecureChannelCredentials()))
  , stub(faabric::FunctionRPCService::NewStub(channel))
{}

void FunctionCallClient::shareFunctionCall(const faabric::Message& call)
{
    ClientContext context;
    faabric::FunctionStatusResponse response;
    CHECK_RPC("function_share", stub->ShareFunction(&context, call, &response));
}

void FunctionCallClient::sendFlush()
{
    ClientContext context;

    faabric::Message call;
    faabric::FunctionStatusResponse response;
    CHECK_RPC("function_flush", stub->Flush(&context, call, &response));
}

void FunctionCallClient::sendMPIMessage(const faabric::MPIMessage& msg)
{
    ClientContext context;
    faabric::FunctionStatusResponse response;
    CHECK_RPC("mpi_message", stub->MPICall(&context, msg, &response));
}

faabric::HostResources FunctionCallClient::getResources(
  const faabric::ResourceRequest& req)
{
    ClientContext context;
    faabric::HostResources response;
    CHECK_RPC("resource_request", stub->GetResources(&context, req, &response));

    return response;
}

void FunctionCallClient::executeFunctions(
  const faabric::BatchExecuteRequest& req)
{
    ClientContext context;
    faabric::FunctionStatusResponse response;
    CHECK_RPC("batch_execute",
              stub->ExecuteFunctions(&context, req, &response));
}

void FunctionCallClient::unregister(const faabric::UnregisterRequest& req)
{
    ClientContext context;
    faabric::FunctionStatusResponse response;
    CHECK_RPC("unregister", stub->Unregister(&context, req, &response));
}
}
