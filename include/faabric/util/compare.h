#pragma once

#include <stdexcept>
#include <vector>

namespace faabric::util {
template<typename T>
bool compareArrays(T* v1, T* v2, int size)
{
    for (int i = 0; i < size; i++) {
        if (v1[i] != v2[i]) {
            return false;
        }
    }

    return true;
}
}
