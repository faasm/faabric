#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallApi.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/util/concurrent_map.h>
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

std::vector<std::pair<std::string, std::shared_ptr<faabric::Message>>>
getMessageResults();

void clearMockRequests();

// -----------------------------------
// Function Call Client
// -----------------------------------

/*
 * The function call client is used to interact with the function call server,
 * faabric's RPC like client/server implementation
 */
class FunctionCallClient : public faabric::transport::MessageEndpointClient
{
  public:
    explicit FunctionCallClient(const std::string& hostIn);

    void sendFlush();

    void executeFunctions(std::shared_ptr<faabric::BatchExecuteRequest> req);

    void setMessageResult(std::shared_ptr<faabric::Message> msg);
};

// -----------------------------------
// Static setter/getters
// -----------------------------------

std::shared_ptr<FunctionCallClient> getFunctionCallClient(
  const std::string& otherHost);

void clearFunctionCallClients();
}
