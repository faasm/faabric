#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/FunctionCallServer.h>

#include <faabric/rpc/macros.h>
#include <faabric/util/queue.h>
#include <faabric/util/testing.h>

#include <faabric/util/logging.h>

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

void clearMockRequests()
{
    functionCalls.clear();
    batchMessages.clear();
    mpiMessages.clear();
    resourceRequests.clear();
    unregisterRequests.clear();

    for (auto& p : queuedResourceResponses) {
        p.second.reset();
    }
    queuedResourceResponses.clear();
}

// -----------------------------------
// Message Client
// -----------------------------------
FunctionCallClient::FunctionCallClient(const std::string& hostIn)
  : faabric::transport::MessageEndpoint(hostIn, FUNCTION_CALL_PORT)
{}

FunctionCallClient::FunctionCallClient(
  faabric::transport::MessageContext& context,
  const std::string& hostIn)
  : faabric::transport::MessageEndpoint(hostIn, FUNCTION_CALL_PORT)
{
    this->open(context, faabric::transport::SocketType::PUSH, false);
}

FunctionCallClient::~FunctionCallClient()
{
    this->close();
}

void FunctionCallClient::close()
{
    MessageEndpoint::close();
}

void FunctionCallClient::sendFlush()
{
    /*
    faabric::Message call;
    if (faabric::util::isMockMode()) {
        flushCalls.emplace_back(host, call);
    } else {
        ClientContext context;
        faabric::FunctionStatusResponse response;
        CHECK_RPC("function_flush", stub->Flush(&context, call, &response));
    }
    */
}

void FunctionCallClient::sendMPIMessage(
  const std::shared_ptr<faabric::MPIMessage> msg)
{
    if (faabric::util::isMockMode()) {
        mpiMessages.emplace_back(host, *msg);
    } else {
        // Send the header first
        // Deliberately using heap allocation, so that ZeroMQ can use zero-copy
        int functionNum =
          static_cast<int>(faabric::scheduler::FunctionCalls::MpiMessage);
        size_t headerSize = sizeof(faabric::scheduler::FunctionCalls);
        char* header = new char[headerSize];
        memcpy(header, &functionNum, headerSize);
        // Mark that we are sending more messages
        send(header, headerSize, true);

        // Send the message body
        size_t msgSize = msg->ByteSizeLong();
        char* serialisedMsg = new char[msgSize];
        // Serialise using protobuf
        if (!msg->SerializeToArray(serialisedMsg, msgSize)) {
            throw std::runtime_error("Error serialising message");
        }
        send(serialisedMsg, msgSize);
    }
}

faabric::HostResources FunctionCallClient::getResources(
  const faabric::ResourceRequest& req)
{
    faabric::HostResources response;
    /*

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

    */
    return response;
}

void FunctionCallClient::executeFunctions(
  const faabric::BatchExecuteRequest& req)
{
    /*
    if (faabric::util::isMockMode()) {
        batchMessages.emplace_back(host, req);
    } else {
        ClientContext context;
        faabric::FunctionStatusResponse response;
        CHECK_RPC("exec_funcs",
                  stub->ExecuteFunctions(&context, req, &response));
    }
    */
}

void FunctionCallClient::unregister(const faabric::UnregisterRequest& req)
{
    /*
    if (faabric::util::isMockMode()) {
        unregisterRequests.emplace_back(host, req);
    } else {
        ClientContext context;
        faabric::FunctionStatusResponse response;
        CHECK_RPC("unregister", stub->Unregister(&context, req, &response));
    }
    */
}

void FunctionCallClient::doRecv(const void* msgData, int size)
{
    throw std::runtime_error("Calling recv from a producer client.");
}
}
