#pragma once

#define BYTES(arr) reinterpret_cast<uint8_t*>(arr)
#define BYTES_CONST(arr) reinterpret_cast<const uint8_t*>(arr)

#define UNUSED(x) (void)(x)
