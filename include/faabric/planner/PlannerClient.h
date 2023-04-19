#pragma once

#include <faabric/planner/planner.pb.h>
#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/util/PeriodicBackgroundThread.h>
#include <faabric/util/scheduling.h>

namespace faabric::planner {
/* The planner's implementation of group membership requires clients to send
 * keep-alive messages. Once started, this background thread will send these
 * messages
 */
class KeepAliveThread : public faabric::util::PeriodicBackgroundThread
{
  public:
    void doWork() override;

    // Register request that we can re-use at every check period
    std::shared_ptr<RegisterHostRequest> thisHostReq = nullptr;
};

class PlannerClient final : public faabric::transport::MessageEndpointClient
{
  public:
    PlannerClient();

    void ping();

    // ------
    // Host membership calls
    // ------

    std::vector<Host> getAvailableHosts();

    // Registering a host returns the keep-alive timeout for heartbeats
    int registerHost(std::shared_ptr<RegisterHostRequest> req);

    void removeHost(std::shared_ptr<RemoveHostRequest> req);

    // ------
    // Scheduling calls
    // ------

    faabric::util::SchedulingDecision callFunctions(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    void setMessageResult(std::shared_ptr<faabric::Message> msg);

    std::shared_ptr<faabric::Message> getMessageResult(
      std::shared_ptr<faabric::Message> msg);
};
}
