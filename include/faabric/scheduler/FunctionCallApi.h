#pragma once

namespace faabric::scheduler {
enum FunctionCalls
{
    NoFunctionCall = 0,
    ExecuteFunctions = 1,
    Flush = 2,
    SetMessageResult = 3,
};
}
