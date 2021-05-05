#pragma once

#define DEFAULT_STATE_HOST "0.0.0.0"
#define STATE_PORT 8003
#define REPLY_PORT_OFFSET 100

namespace faabric::state {
enum StateCalls
{
    None = 0,
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
