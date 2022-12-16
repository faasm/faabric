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

std::vector<std::pair<std::string, std::shared_ptr<faabric::PendingMigrations>>>
getPendingMigrationsRequests();

std::vector<std::pair<std::string, faabric::UnregisterRequest>>
getUnregisterRequests();

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

    void sendAddPendingMigrations(
      std::shared_ptr<faabric::PendingMigrations> req);

    void sendRemovePendingMigrations(
      std::shared_ptr<faabric::PendingMigrations> req);

    void executeFunctions(std::shared_ptr<faabric::BatchExecuteRequest> req);

    void unregister(faabric::UnregisterRequest& req);
};
}
