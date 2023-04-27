#pragma once

#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/util/scheduling.h>

#include <map>

namespace faabric::planner {
typedef std::pair<std::shared_ptr<faabric::BatchExecuteRequest>,
                  std::shared_ptr<faabric::util::SchedulingDecision>>
  InFlightRequest;

enum DecisionType
{
    NO_DECISION_TYPE = 0,
    NEW = 1,
    DIST_CHANGE = 2,
    SCALE_CHANGE = 3,
};

/* This helper struct encapsulates the internal state of the planner
 */
struct PlannerState
{
    // Accounting of the hosts that are registered in the system and responsive
    // We deliberately use the host's IP as unique key, but assign a unique host
    // id for redundancy
    std::map<std::string, std::shared_ptr<Host>> hostMap;

    // Map of the in-flight requests indexed by the app id
    std::map<int, InFlightRequest> inFlightRequests;

    // Double-map holding the message results. The first key is the app id. For
    // each app id, we keep a map of the message id, and the actual message
    // result
    std::map<int, std::map<int, std::shared_ptr<faabric::Message>>> appResults;

    // Map holding the hosts that have registered interest in getting an app
    // result
    std::map<int, std::vector<std::string>> appResultWaiters;
};
}
