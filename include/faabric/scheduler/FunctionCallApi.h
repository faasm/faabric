#pragma once

namespace faabric::scheduler {
enum FunctionCalls
{
    NoFunctionCall = 0,
    MpiMessage = 1,
    ExecuteFunctions = 2,
    Flush = 3,
    Unregister = 4,
    GetResources = 5,
    SetThreadResult = 6,
};
}
