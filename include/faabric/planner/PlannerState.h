#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>

#include <map>

namespace faabric::planner {
/* This helper struct encapsulates the internal state of the planner
 */
struct PlannerState
{
    // Policy to operate the planner in. Mostly determins the batch scheduler
    // behaviour, but also the planner's in some cases
    std::string policy;

    // Accounting of the hosts that are registered in the system and responsive
    // We deliberately use the host's IP as unique key, but assign a unique host
    // id for redundancy
    std::map<std::string, std::shared_ptr<Host>> hostMap;

    // Double-map holding the message results. The first key is the app id. For
    // each app id, we keep a map of the message id, and the actual message
    // result
    std::map<int, std::map<int, std::shared_ptr<faabric::Message>>> appResults;

    // Map holding the hosts that have registered interest in getting an app
    // result
    std::map<int, std::vector<std::string>> appResultWaiters;

    // Map keeping track of the requests that are in-flight
    faabric::batch_scheduler::InFlightReqs inFlightReqs;

    // Map keeping track of pre-loaded scheduling decisions that bypass the
    // planner's scheduling
    std::map<int, std::shared_ptr<batch_scheduler::SchedulingDecision>>
      preloadedSchedulingDecisions;

    // Helper coutner of the total number of migrations
    std::atomic<int> numMigrations = 0;

    // -----
    // Data structures used only under the SPOT policy
    // -----

    // Map containing the BER that have been evicted due to a SPOT VM eviction.
    // All messages in the VM have been checkpointed, are in the snapshot
    // registry in the planner, and are ready to be scheduled when capacity
    // appears
    std::map<int, std::shared_ptr<BatchExecuteRequest>> evictedRequests;

    // This variable simulates the values we would get from a cloud provider's
    // API indicating the (set of) VM to be evicted next
    std::set<std::string> nextEvictedHostIps;
};
}
