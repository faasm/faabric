#pragma once

namespace faabric::transport {

enum PointToPointCall
{
    MAPPING = 0,
    MESSAGE = 1,
    LOCK_GROUP = 2,
    LOCK_GROUP_RECURSIVE = 3,
    UNLOCK_GROUP = 4,
    UNLOCK_GROUP_RECURSIVE = 5,
};
}
