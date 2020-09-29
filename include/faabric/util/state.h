#pragma once

#include <string>

#define STATE_MASK_8 0b11111111
#define STATE_MASK_32 0b11111111111111111111111111111111

namespace faabric::util {
std::string keyForUser(const std::string& user, const std::string& key);

void maskDouble(unsigned int* maskArray, unsigned long idx);
}
