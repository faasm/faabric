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

faabric::planner::PlannerConfig getPlannerConfig()
{
    faabric::planner::HttpMessage msg;
    msg.set_type(faabric::planner::HttpMessage_Type_GET_CONFIG);
    std::string jsonStr = faabric::util::messageToJson(msg);

    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    std::pair<int, std::string> result =
      postToUrl(conf.plannerHost, conf.plannerPort, jsonStr);
    REQUIRE(result.first == 200);

    // Check that we can de-serialise the config. Note that if there's a
    // de-serialisation the method will throw an exception
    faabric::planner::PlannerConfig config;
    faabric::util::jsonToMessage(result.second, &config);
    return config;
}

void flushExecutors()
{
    faabric::planner::HttpMessage msg;
    msg.set_type(faabric::planner::HttpMessage_Type_FLUSH_EXECUTORS);
    std::string jsonStr = faabric::util::messageToJson(msg);

    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    std::pair<int, std::string> result =
      postToUrl(conf.plannerHost, conf.plannerPort, jsonStr);
    REQUIRE(result.first == 200);
}
}
