#pragma once

#include <faabric/planner/planner.pb.h>
#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/util/PeriodicBackgroundThread.h>

namespace faabric::planner {
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

    std::vector<Host> getAvailableHosts();

    // Registering a host returns the keep-alive timeout for heartbeats, and
    // the unique host id
    std::pair<int, int> registerHost(std::shared_ptr<RegisterHostRequest> req);

    // Remove host is an asynchronous request that will try to remove the host
    // pointed-to by the remove request
    void removeHost(std::shared_ptr<RemoveHostRequest> req);
};
}
