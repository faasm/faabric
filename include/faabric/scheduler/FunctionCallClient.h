#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageContext.h>
#include <faabric/transport/SimpleMessageEndpoint.h>
#include <faabric/util/config.h>

namespace faabric::scheduler {

// -----------------------------------
// Mocking
// -----------------------------------
std::vector<std::pair<std::string, faabric::Message>> getFunctionCalls();

std::vector<std::pair<std::string, faabric::Message>> getFlushCalls();

std::vector<std::pair<std::string, faabric::BatchExecuteRequest>>
getBatchRequests();

std::vector<std::pair<std::string, faabric::MPIMessage>> getMPIMessages();

std::vector<std::pair<std::string, faabric::ResourceRequest>>
getResourceRequests();

std::vector<std::pair<std::string, faabric::UnregisterRequest>>
getUnregisterRequests();

void queueResourceResponse(const std::string& host,
                           faabric::HostResources& res);

void clearMockRequests();

// -----------------------------------
// Message client
// -----------------------------------
class FunctionCallClient : faabric::transport::SimpleMessageEndpoint
{
  public:
    explicit FunctionCallClient(const std::string& hostIn);

    ~FunctionCallClient();

    void close();

    void sendFlush();

    void sendMPIMessage(const std::shared_ptr<faabric::MPIMessage> msg);

    faabric::HostResources getResources(const faabric::ResourceRequest& req);

    void executeFunctions(const faabric::BatchExecuteRequest& req);

    void unregister(const faabric::UnregisterRequest& req);

  private:
    void doRecv(void* msgData, int size) override;

    void sendHeader(faabric::scheduler::FunctionCalls call);

    void awaitResponse();

    void awaitResponse(char*& data, int& size);
};
}
