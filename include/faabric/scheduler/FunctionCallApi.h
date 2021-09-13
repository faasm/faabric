#pragma once

namespace faabric::scheduler {
enum FunctionCalls
{
    NoFunctionCall = 0,
    ExecuteFunctions = 1,
    Flush = 2,
    Unregister = 3,
    GetResources = 4,
    GroupLock = 5,
    GroupUnlock = 6,
    GroupNotify = 7,
    GroupBarrier = 8,
};
}
