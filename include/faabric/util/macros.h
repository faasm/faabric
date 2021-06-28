#pragma once

#include <unistd.h>

#define BYTES(arr) reinterpret_cast<uint8_t*>(arr)
#define BYTES_CONST(arr) reinterpret_cast<const uint8_t*>(arr)

#define SLEEP_MS(ms) usleep((ms) * 1000)

#define UNUSED(x) (void)(x)
