#pragma once

#include <string>

#define LOCALHOST "127.0.0.1"

namespace faabric::util {
std::string getIPFromHostname(const std::string& hostname);

std::string getPrimaryIPForThisHost(const std::string& interface);
}