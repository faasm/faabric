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

void updatePlannerPolicy(const std::string& newPolicy)
{
    faabric::planner::HttpMessage msg;
    msg.set_type(faabric::planner::HttpMessage_Type_SET_POLICY);
    msg.set_payloadjson(newPolicy);
    std::string jsonStr = faabric::util::messageToJson(msg);

    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    std::pair<int, std::string> result =
      postToUrl(conf.plannerHost, conf.plannerPort, jsonStr);
    assert(result.first == 200);
}

void setNextEvictedVmIp(const std::set<std::string>& evictedVmIps)
{
    faabric::planner::HttpMessage msg;
    msg.set_type(faabric::planner::HttpMessage_Type_SET_NEXT_EVICTED_VM);

    faabric::planner::SetEvictedVmIpsRequest evictedRequest;
    for (const auto& evictedIp : evictedVmIps) {
        evictedRequest.add_vmips(evictedIp);
    }
    msg.set_payloadjson(faabric::util::messageToJson(evictedRequest));
    std::string jsonStr = faabric::util::messageToJson(msg);

    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    std::pair<int, std::string> result =
      postToUrl(conf.plannerHost, conf.plannerPort, jsonStr);
    assert(result.first == 200);
}

faabric::planner::GetInFlightAppsResponse getInFlightApps()
{
    faabric::planner::HttpMessage msg;
    msg.set_type(faabric::planner::HttpMessage_Type_GET_IN_FLIGHT_APPS);
    std::string jsonStr = faabric::util::messageToJson(msg);

    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    std::pair<int, std::string> result =
      postToUrl(conf.plannerHost, conf.plannerPort, jsonStr);
    assert(result.first == 200);

    faabric::planner::GetInFlightAppsResponse response;
    faabric::util::jsonToMessage(result.second, &response);

    return response;
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
