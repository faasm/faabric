#include <faabric/planner/Planner.h>
#include <faabric/util/logging.h>

namespace faabric::planner {
Planner::Planner()
{
    SPDLOG_INFO("Hello world!");
}

Planner& getPlanner()
{
    static Planner planner;
    return planner;
}
}
