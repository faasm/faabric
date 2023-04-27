#pragma once

namespace faabric::scheduler {
enum FunctionCalls
{
    NoFunctionCall = 0,
    ExecuteFunctions = 1,
    Flush = 2,
    // TODO: remove this two?
    Unregister = 3,
    GetResources = 4,
    SetMessageResult = 5,
};
}
