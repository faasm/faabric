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
FunctionCallClient::FunctionCallClient(const std::string& hostIn)
  : faabric::transport::SimpleMessageEndpoint(hostIn, FUNCTION_CALL_PORT)
{
    this->open(faabric::transport::getGlobalMessageContext(),
               faabric::transport::SocketType::PUSH,
               false);
}

FunctionCallClient::~FunctionCallClient()
{
    if (!faabric::util::isMockMode()) {
        this->close();
    }
}

void FunctionCallClient::close()
{
    SimpleMessageEndpoint::close();
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

void FunctionCallClient::awaitResponse()
{
    char* data;
    int size;
    awaitResponse(data, size);
}

void FunctionCallClient::awaitResponse(char*& data, int& size)
{
    // Call the superclass implementation
    SimpleMessageEndpoint::awaitResponse(faabric::util::getSystemConfig().endpointHost,
                                         FUNCTION_CALL_PORT + REPLY_PORT_OFFSET,
                                         data,
                                         size);
}


void FunctionCallClient::sendFlush()
{
    if (faabric::util::isMockMode()) {
        faabric::Message call;
        flushCalls.emplace_back(host, call);
    } else {
        faabric::ResponseRequest call;

        // Send the header first
        sendHeader(faabric::scheduler::FunctionCalls::Flush);

        // Send the message body
        call.set_returnhost(faabric::util::getSystemConfig().endpointHost);
        size_t msgSize = call.ByteSizeLong();
        char* serialisedMsg = new char[msgSize];
        // Serialise using protobuf
        if (!call.SerializeToArray(serialisedMsg, msgSize)) {
            throw std::runtime_error("Error serialising message");
        }
        send(serialisedMsg, msgSize);

        awaitResponse();
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
    if (faabric::util::isMockMode()) {
        // Register the request
        resourceRequests.emplace_back(host, req);

        // See if we have a queued response
        if (queuedResourceResponses[host].size() > 0) {
            response = queuedResourceResponses[host].dequeue();
        }
    } else {
        // Send the header first
        sendHeader(faabric::scheduler::FunctionCalls::GetResources);

        faabric::ResponseRequest request;
        request.set_returnhost(faabric::util::getSystemConfig().endpointHost);
        size_t msgSize = request.ByteSizeLong();
        char* serialisedMsg = new char[msgSize];
        // Serialise using protobuf
        if (!request.SerializeToArray(serialisedMsg, msgSize)) {
            throw std::runtime_error("Error serialising message");
        }
        send(serialisedMsg, msgSize);

        // Receive message
        char* msgData;
        int size;
        awaitResponse(msgData, size);
        // Deserialise message string
        if (!response.ParseFromArray(msgData, size)) {
            throw std::runtime_error("Error deserialising message");
        }
    }

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

void FunctionCallClient::doRecv(void* msgData, int size)
{
    throw std::runtime_error("Calling recv from a producer client.");
}
}
