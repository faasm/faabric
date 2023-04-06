#pragma once

#include <faabric/planner/planner.pb.h>

#include <map>

namespace faabric::planner {
/* This helper struct encapsulates the internal state of the planner
 */
struct PlannerState
{
    // Accounting of the hosts that are registered in the system and responsive
    // We deliberately use the host's IP as unique key, as if two "hosts" have
    // the same IP, they are the same host
    // TODO: should we give a UUID too?
    std::map<std::string, std::shared_ptr<Host>> hostMap;
};
}
