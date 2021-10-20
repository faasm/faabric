#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallApi.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/queue.h>
#include <faabric/util/testing.h>

namespace faabric::scheduler {

// -----------------------------------
// Mocking
// -----------------------------------
std::mutex mockMutex;

static std::vector<std::pair<std::string, faabric::Message>> functionCalls;

static std::vector<std::pair<std::string, faabric::EmptyRequest>> flushCalls;

static std::vector<
  std::pair<std::string, std::shared_ptr<faabric::BatchExecuteRequest>>>
  batchMessages;

static std::vector<std::pair<std::string, faabric::EmptyRequest>>
  resourceRequests;

static std::unordered_map<std::string,
                          faabric::util::Queue<faabric::HostResources>>
  queuedResourceResponses;

static std::vector<std::pair<std::string, faabric::UnregisterRequest>>
  unregisterRequests;

static std::vector<std::pair<std::string, faabric::CoordinationRequest>>
  coordinationRequests;

std::vector<std::pair<std::string, faabric::Message>> getFunctionCalls()
{
    return functionCalls;
}

std::vector<std::pair<std::string, faabric::EmptyRequest>> getFlushCalls()
{
    return flushCalls;
}

std::vector<
  std::pair<std::string, std::shared_ptr<faabric::BatchExecuteRequest>>>
getBatchRequests()
{
    return batchMessages;
}

std::vector<std::pair<std::string, faabric::EmptyRequest>> getResourceRequests()
{
    return resourceRequests;
}

std::vector<std::pair<std::string, faabric::UnregisterRequest>>
getUnregisterRequests()
{
    return unregisterRequests;
}

std::vector<std::pair<std::string, faabric::CoordinationRequest>>
getCoordinationRequests()
{
    return coordinationRequests;
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
    coordinationRequests.clear();

    for (auto& p : queuedResourceResponses) {
        p.second.reset();
    }
    queuedResourceResponses.clear();
}

// -----------------------------------
// Message Client
// -----------------------------------
FunctionCallClient::FunctionCallClient(const std::string& hostIn)
  : faabric::transport::MessageEndpointClient(hostIn,
                                              FUNCTION_CALL_ASYNC_PORT,
                                              FUNCTION_CALL_SYNC_PORT)
{}

void FunctionCallClient::sendFlush()
{
    faabric::EmptyRequest req;
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        flushCalls.emplace_back(host, req);
    } else {
        faabric::EmptyResponse resp;
        syncSend(faabric::scheduler::FunctionCalls::Flush, &req, &resp);
    }
}

faabric::HostResources FunctionCallClient::getResources()
{
    faabric::EmptyRequest request;
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
        syncSend(
          faabric::scheduler::FunctionCalls::GetResources, &request, &response);
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
        asyncSend(faabric::scheduler::FunctionCalls::ExecuteFunctions,
                  req.get());
    }
}

void FunctionCallClient::unregister(faabric::UnregisterRequest& req)
{
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        unregisterRequests.emplace_back(host, req);
    } else {
        asyncSend(faabric::scheduler::FunctionCalls::Unregister, &req);
    }
}

void FunctionCallClient::makeCoordinationRequest(
  int32_t groupId,
  int32_t groupSize,
  int32_t groupIdx,
  bool recursive,
  faabric::scheduler::FunctionCalls call)
{
    faabric::CoordinationRequest req;
    req.set_groupid(groupId);
    req.set_groupsize(groupSize);
    req.set_groupidx(groupIdx);
    req.set_recursive(recursive);

    faabric::CoordinationRequest::CoordinationOperation op;
    switch (call) {
        case (faabric::scheduler::FunctionCalls::GroupLock): {
            SPDLOG_TRACE("Requesting lock on {} at {}", groupId, host);
            op = faabric::CoordinationRequest::LOCK;
            break;
        }
        case (faabric::scheduler::FunctionCalls::GroupUnlock): {
            SPDLOG_TRACE("Requesting unlock on {} at {}", groupId, host);
            op = faabric::CoordinationRequest::UNLOCK;
            break;
        }
        case (faabric::scheduler::FunctionCalls::GroupNotify): {
            SPDLOG_TRACE("Requesting notify on {} at {}", groupId, host);
            op = faabric::CoordinationRequest::NOTIFY;
            break;
        }
        case (faabric::scheduler::FunctionCalls::GroupBarrier): {
            SPDLOG_TRACE("Requesting barrier {} at {}", groupId, host);
            op = faabric::CoordinationRequest::BARRIER;
            break;
        }
        default: {
            SPDLOG_ERROR("Invalid function group call {}", call);
            throw std::runtime_error("Invalid function group call");
        }
    }

    req.set_operation(op);

    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        coordinationRequests.emplace_back(host, req);
    } else {
        asyncSend(call, &req);
    }
}

void FunctionCallClient::coordinationLock(int32_t groupId,
                                          int32_t groupSize,
                                          int32_t groupIdx,
                                          bool recursive)
{
    makeCoordinationRequest(groupId,
                            groupSize,
                            groupIdx,
                            recursive,
                            faabric::scheduler::FunctionCalls::GroupLock);
}

void FunctionCallClient::coordinationUnlock(int32_t groupId,
                                            int32_t groupSize,
                                            int32_t groupIdx,
                                            bool recursive)
{
    makeCoordinationRequest(groupId,
                            groupSize,
                            groupIdx,
                            recursive,
                            faabric::scheduler::FunctionCalls::GroupUnlock);
}

void FunctionCallClient::coordinationNotify(int32_t groupId,
                                            int32_t groupSize,
                                            int32_t groupIdx)
{
    makeCoordinationRequest(groupId,
                            groupSize,
                            groupIdx,
                            false,
                            faabric::scheduler::FunctionCalls::GroupNotify);
}

void FunctionCallClient::coordinationBarrier(int32_t groupId,
                                             int32_t groupSize,
                                             int32_t groupIdx)
{
    makeCoordinationRequest(groupId,
                            groupSize,
                            groupIdx,
                            false,
                            faabric::scheduler::FunctionCalls::GroupBarrier);
}

}
