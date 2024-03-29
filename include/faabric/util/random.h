#pragma once

#include <string>
#include <unordered_set>

namespace faabric::util {
std::string randomString(int len);

std::string randomStringFromSet(const std::unordered_set<std::string>& s);
}
