#pragma once

#include <string>

namespace faabric::util {

struct SnapshotData
{
    size_t size = 0;
    const uint8_t* data = nullptr;
    int fd = 0;
};
}
