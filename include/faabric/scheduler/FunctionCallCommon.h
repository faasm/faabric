#pragma once

#define DEFAULT_RPC_HOST "0.0.0.0"
#define STATE_PORT 8003
#define FUNCTION_CALL_PORT 8004
#define MPI_MESSAGE_PORT 8005
#define SNAPSHOT_RPC_PORT 8006
#define REPLY_PORT_OFFSET 100

namespace faabric::scheduler {
enum FunctionCalls
{
    None = 0,
    MpiMessage = 1,
    ExecuteFunctions = 2,
    Flush = 3,
    Unregister = 4,
    GetResources = 5,
};
}
