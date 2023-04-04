#pragma once

namespace faabric::planner {
enum PlannerCalls
{
    NoPlanerCall = 0,
    Ping = 1,
    GetAvailableHosts = 2,
    RegisterHost = 3,
    RemoveHost = 4,
};
}
