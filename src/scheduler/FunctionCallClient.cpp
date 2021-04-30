#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>

#include <faabric/rpc/macros.h>
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

// TODO - remove this constructor?
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
    if (!faabric::util::isMockMode()) {
        this->close();
    }
}

void FunctionCallClient::close()
{
    MessageEndpoint::close();
}

void FunctionCallClient::sendHeader(faabric::scheduler::FunctionCalls call)
{
    // Deliberately using heap allocation, so that ZeroMQ can use zero-copy
    int functionNum = static_cast<int>(call);
    size_t headerSize = sizeof(faabric::scheduler::FunctionCalls);
    char* header = new char[headerSize];
    memcpy(header, &functionNum, headerSize);
    // Mark that we are sending more messages
    send(header, headerSize, true);
}

void FunctionCallClient::sendFlush()
{
    faabric::Message call;
    if (faabric::util::isMockMode()) {
        flushCalls.emplace_back(host, call);
    } else {
        // Send the header first
        sendHeader(faabric::scheduler::FunctionCalls::Flush);

        // Send the message body
        size_t msgSize = call.ByteSizeLong();
        char* serialisedMsg = new char[msgSize];
        // Serialise using protobuf
        if (!call.SerializeToArray(serialisedMsg, msgSize)) {
            throw std::runtime_error("Error serialising message");
        }
        send(serialisedMsg, msgSize);
    }
}

void FunctionCallClient::sendMPIMessage(
  const std::shared_ptr<faabric::MPIMessage> msg)
{
    if (faabric::util::isMockMode()) {
        mpiMessages.emplace_back(host, *msg);
    } else {
        // Send the header first
        sendHeader(faabric::scheduler::FunctionCalls::MpiMessage);

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
    if (faabric::util::isMockMode()) {
        batchMessages.emplace_back(host, req);
    } else {
        // Send the header first
        sendHeader(faabric::scheduler::FunctionCalls::ExecuteFunctions);

        // Send the message body
        size_t msgSize = req.ByteSizeLong();
        char* serialisedMsg = new char[msgSize];
        // Serialise using protobuf
        if (!req.SerializeToArray(serialisedMsg, msgSize)) {
            throw std::runtime_error("Error serialising message");
        }
        send(serialisedMsg, msgSize);
    }
}

void FunctionCallClient::unregister(const faabric::UnregisterRequest& req)
{
    if (faabric::util::isMockMode()) {
        unregisterRequests.emplace_back(host, req);
    } else {
        // Send the header first
        sendHeader(faabric::scheduler::FunctionCalls::Unregister);

        // Send the message body
        size_t msgSize = req.ByteSizeLong();
        char* serialisedMsg = new char[msgSize];
        // Serialise using protobuf
        if (!req.SerializeToArray(serialisedMsg, msgSize)) {
            throw std::runtime_error("Error serialising message");
        }
        send(serialisedMsg, msgSize);
    }
}

void FunctionCallClient::doRecv(const void* msgData, int size)
{
    throw std::runtime_error("Calling recv from a producer client.");
}
}
