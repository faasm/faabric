#pragma once

#define DEFAULT_STATE_HOST "0.0.0.0"
#define DEFAULT_FUNCTION_CALL_HOST "0.0.0.0"
#define DEFAULT_SNAPSHOT_HOST "0.0.0.0"
#define STATE_PORT 8003
#define FUNCTION_CALL_PORT 8004
#define MPI_MESSAGE_PORT 8005
#define SNAPSHOT_PORT 8006
#define REPLY_PORT_OFFSET 100

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

enum SnapshotCalls
{
    NoSnapshotCall = 0,
    PushSnapshot = 1,
    DeleteSnapshot = 2,
};
}

namespace faabric::state {
enum StateCalls
{
    NoStateCall = 0,
    Pull = 1,
    Push = 2,
    Size = 3,
    Append = 4,
    ClearAppended = 5,
    PullAppended = 6,
    Lock = 7,
    Unlock = 8,
    Delete = 9,
};
}
