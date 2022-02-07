#pragma once

namespace faabric::scheduler {
enum FunctionCalls
{
    NoFunctionCall = 0,
    ExecuteFunctions = 1,
    Flush = 2,
    Unregister = 3,
    GetResources = 4,
    PendingMigrations = 5
};
}
