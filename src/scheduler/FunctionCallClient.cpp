#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/transport/macros.h>
#include <faabric/util/queue.h>
#include <faabric/util/testing.h>

namespace faabric::scheduler {

// -----------------------------------
// Mocking
// -----------------------------------
std::mutex mockMutex;

static std::vector<std::pair<std::string, faabric::Message>> functionCalls;

static std::vector<std::pair<std::string, faabric::ResponseRequest>> flushCalls;

static std::vector<
  std::pair<std::string, std::shared_ptr<faabric::BatchExecuteRequest>>>
  batchMessages;

static std::vector<std::pair<std::string, faabric::ResponseRequest>>
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

std::vector<std::pair<std::string, faabric::ResponseRequest>> getFlushCalls()
{
    return flushCalls;
}

std::vector<
  std::pair<std::string, std::shared_ptr<faabric::BatchExecuteRequest>>>
getBatchRequests()
{
    return batchMessages;
}

std::vector<std::pair<std::string, faabric::ResponseRequest>>
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
  : faabric::transport::MessageEndpointClient(hostIn, FUNCTION_CALL_PORT)
{
}

void FunctionCallClient::sendHeader(faabric::scheduler::FunctionCalls call)
{
    uint8_t header = static_cast<uint8_t>(call);
    send(&header, sizeof(header), true);
}

void FunctionCallClient::sendFlush()
{
    faabric::ResponseRequest call;
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        flushCalls.emplace_back(host, call);
    } else {
        // Prepare the message body
        call.set_returnhost(faabric::util::getSystemConfig().endpointHost);

        SEND_MESSAGE(faabric::scheduler::FunctionCalls::Flush, call);

        // Await the response
        awaitResponse(FUNCTION_CALL_PORT + REPLY_PORT_OFFSET);
    }
}

faabric::HostResources FunctionCallClient::getResources()
{
    faabric::ResponseRequest request;
    faabric::HostResources response;
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);

        // Register the request
        resourceRequests.emplace_back(host, request);

        // See if we have a queued response
        if (queuedResourceResponses[host].size() > 0) {
            response = queuedResourceResponses[host].dequeue();
        }
    } else {
        request.set_returnhost(faabric::util::getSystemConfig().endpointHost);

        SEND_MESSAGE(faabric::scheduler::FunctionCalls::GetResources, request);

        // Receive message
        faabric::transport::Message msg =
          awaitResponse(FUNCTION_CALL_PORT + REPLY_PORT_OFFSET);
        // Deserialise message string
        if (!response.ParseFromArray(msg.data(), msg.size())) {
            throw std::runtime_error("Error deserialising message");
        }
    }

    return response;
}

void FunctionCallClient::executeFunctions(
  const std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        batchMessages.emplace_back(host, req);
    } else {
        SEND_MESSAGE_PTR(faabric::scheduler::FunctionCalls::ExecuteFunctions,
                         req);
    }
}

void FunctionCallClient::unregister(const faabric::UnregisterRequest& req)
{
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        unregisterRequests.emplace_back(host, req);
    } else {
        SEND_MESSAGE(faabric::scheduler::FunctionCalls::Unregister, req);
    }
}
}
