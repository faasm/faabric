#pragma once

#include <unistd.h>

template<class T>
uint8_t* BYTES(T* arr)
{
    return reinterpret_cast<uint8_t*>(arr);
}

template<class T>
const uint8_t* BYTES_CONST(const T* arr)
{
    return reinterpret_cast<const uint8_t*>(arr);
}

#define SLEEP_MS(ms) usleep((ms) * 1000)

#define UNUSED(x) (void)(x)
