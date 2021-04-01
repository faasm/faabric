#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <faabric/proto/macros.h>
#include <faabric/util/queue.h>
#include <faabric/util/testing.h>

namespace faabric::scheduler {

// -----------------------------------
// Mocking
// -----------------------------------
static std::vector<std::pair<std::string, faabric::Message>> functionCalls;

static std::vector<std::pair<std::string, faabric::Message>> flushCalls;

static std::vector<std::pair<std::string, faabric::BatchExecuteRequest>>
  batchMessages;

static std::vector<std::pair<std::string, faabric::MPIMessage>> mpiMessages;

static std::vector<std::pair<std::string, faabric::ResourceRequest>>
  resourceRequests;

static std::unordered_map<std::string,
                          faabric::util::Queue<faabric::HostResources>>
  queuedResourceResponses;

static std::vector<std::pair<std::string, faabric::UnregisterRequest>>
  unregisterRequests;

static std::vector<std::pair<std::string, faabric::SnapshotData>>
  snapshotPushes;

std::vector<std::pair<std::string, faabric::Message>> getFunctionCalls()
{
    return functionCalls;
}

std::vector<std::pair<std::string, faabric::Message>> getFlushCalls()
{
    return flushCalls;
}

std::vector<std::pair<std::string, faabric::BatchExecuteRequest>>
getBatchRequests()
{
    return batchMessages;
}

std::vector<std::pair<std::string, faabric::MPIMessage>> getMPIMessages()
{
    return mpiMessages;
}

std::vector<std::pair<std::string, faabric::ResourceRequest>>
getResourceRequests()
{
    return resourceRequests;
}

std::vector<std::pair<std::string, faabric::UnregisterRequest>>
getUnregisterRequests()
{
    return unregisterRequests;
}

void queueResourceResponse(const std::string& host, faabric::HostResources& res)
{
    queuedResourceResponses[host].enqueue(res);
}

std::vector<std::pair<std::string, faabric::SnapshotData>> getSnapshotPushes()
{
    return snapshotPushes;
}

void clearMockRequests()
{
    functionCalls.clear();
    batchMessages.clear();
    mpiMessages.clear();
    resourceRequests.clear();
    unregisterRequests.clear();
    snapshotPushes.clear();

    for (auto& p : queuedResourceResponses) {
        p.second.reset();
    }
    queuedResourceResponses.clear();
}

// -----------------------------------
// gRPC client
// -----------------------------------
FunctionCallClient::FunctionCallClient(const std::string& hostIn)
  : host(hostIn)
  , channel(grpc::CreateChannel(host + ":" + std::to_string(FUNCTION_CALL_PORT),
                                grpc::InsecureChannelCredentials()))
  , stub(faabric::FunctionRPCService::NewStub(channel))
{}

void FunctionCallClient::sendFlush()
{
    faabric::Message call;
    if (faabric::util::isMockMode()) {
        flushCalls.emplace_back(host, call);
    } else {
        ClientContext context;
        faabric::FunctionStatusResponse response;
        CHECK_RPC("function_flush", stub->Flush(&context, call, &response));
    }
}

void FunctionCallClient::sendMPIMessage(const faabric::MPIMessage& msg)
{
    if (faabric::util::isMockMode()) {
        mpiMessages.emplace_back(host, msg);
    } else {
        ClientContext context;
        faabric::FunctionStatusResponse response;
        CHECK_RPC("mpi_message", stub->MPICall(&context, msg, &response));
    }
}

faabric::HostResources FunctionCallClient::getResources(
  const faabric::ResourceRequest& req)
{
    faabric::HostResources response;

    if (faabric::util::isMockMode()) {
        // Register the request
        resourceRequests.emplace_back(host, req);

        // See if we have a queued response
        if (queuedResourceResponses[host].size() > 0) {
            response = queuedResourceResponses[host].dequeue();
        }
    } else {
        ClientContext context;
        CHECK_RPC("get_resources",
                  stub->GetResources(&context, req, &response));
    }

    return response;
}

void FunctionCallClient::executeFunctions(
  const faabric::BatchExecuteRequest& req)
{
    if (faabric::util::isMockMode()) {
        batchMessages.emplace_back(host, req);
    } else {
        ClientContext context;
        faabric::FunctionStatusResponse response;
        CHECK_RPC("exec_funcs",
                  stub->ExecuteFunctions(&context, req, &response));
    }
}

void FunctionCallClient::unregister(const faabric::UnregisterRequest& req)
{
    if (faabric::util::isMockMode()) {
        unregisterRequests.emplace_back(host, req);
    } else {
        ClientContext context;
        faabric::FunctionStatusResponse response;
        CHECK_RPC("unregister", stub->Unregister(&context, req, &response));
    }
}

void FunctionCallClient::pushSnapshot(const faabric::SnapshotData& data)
{
    if (faabric::util::isMockMode()) {
        snapshotPushes.emplace_back(host, data);
    } else {
        ClientContext context;
        faabric::FunctionStatusResponse response;
        CHECK_RPC("pushSnapshot",
                  stub->PushSnapshot(&context, data, &response));
    }
}
}
