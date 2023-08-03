#pragma once

namespace faabric::planner {
enum PlannerCalls
{
    NoPlanerCall = 0,
    // Util
    Ping = 1,
    // Host-membership calls
    GetAvailableHosts = 2,
    RegisterHost = 3,
    RemoveHost = 4,
    // Scheduling calls
    SetMessageResult = 8,
    GetMessageResult = 9,
    CallBatch = 10,
};
}
