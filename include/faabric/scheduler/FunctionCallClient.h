#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallApi.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/util/config.h>

namespace faabric::scheduler {

// -----------------------------------
// Mocking
// -----------------------------------
std::vector<std::pair<std::string, faabric::Message>> getFunctionCalls();

std::vector<std::pair<std::string, faabric::EmptyRequest>> getFlushCalls();

std::vector<
  std::pair<std::string, std::shared_ptr<faabric::BatchExecuteRequest>>>
getBatchRequests();

std::vector<std::pair<std::string, faabric::EmptyRequest>>
getResourceRequests();

std::vector<std::pair<std::string, faabric::UnregisterRequest>>
getUnregisterRequests();

std::vector<std::pair<std::string, faabric::CoordinationRequest>>
getCoordinationRequests();

void queueResourceResponse(const std::string& host,
                           faabric::HostResources& res);

void clearMockRequests();

// -----------------------------------
// Message client
// -----------------------------------
class FunctionCallClient : public faabric::transport::MessageEndpointClient
{
  public:
    explicit FunctionCallClient(const std::string& hostIn);

    void sendFlush();

    faabric::HostResources getResources();

    void executeFunctions(
      const std::shared_ptr<faabric::BatchExecuteRequest> req);

    void unregister(faabric::UnregisterRequest& req);

    // --- Function group operations ---
    void coordinationLock(const faabric::Message& msg);

    void coordinationUnlock(const faabric::Message& msg);

    void coordinationNotify(const faabric::Message& msg);

    void coordinationBarrier(const faabric::Message& msg);

  private:
    void sendHeader(faabric::scheduler::FunctionCalls call);

    void makeCoordinationRequest(const faabric::Message& msg,
                                 faabric::scheduler::FunctionCalls call);
};
}
