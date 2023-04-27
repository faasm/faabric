#pragma once

namespace faabric::planner {
enum PlannerCalls
{
    NoPlanerCall = 0,
    // Util
    Ping = 1,
    SetTestsConfig = 2,
    // Host-membership calls
    GetAvailableHosts = 3,
    RegisterHost = 4,
    RemoveHost = 5,
    // Scheduling calls
    CallFunctions = 6,
    GetSchedulingDecision = 7,
    SetMessageResult = 8,
    GetMessageResult = 9,
    GetBatchMessages = 10,
};
}
