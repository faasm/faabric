#pragma once

#include <string>

namespace faabric::util {

struct SnapshotData
{
    size_t size = 0;
    uint8_t* data = nullptr;
    int fd = 0;
};
}
