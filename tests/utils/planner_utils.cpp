#include "faabric_utils.h"

#include <faabric/planner/planner.pb.h>
#include <faabric/util/json.h>

namespace tests {
void resetPlanner()
{
    faabric::planner::HttpMessage msg;
    msg.set_type(faabric::planner::HttpMessage_Type_RESET);
    std::string jsonStr = faabric::util::messageToJson(msg);

    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    std::pair<int, std::string> result =
      postToUrl(conf.plannerHost, conf.plannerPort, jsonStr);
    assert(result.first == 200);
}

void flushPlannerWorkers()
{
    faabric::planner::HttpMessage msg;
    msg.set_type(faabric::planner::HttpMessage_Type_FLUSH_EXECUTORS);
    std::string jsonStr = faabric::util::messageToJson(msg);

    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    std::pair<int, std::string> result =
      postToUrl(conf.plannerHost, conf.plannerPort, jsonStr);
    assert(result.first == 200);
}
}
