#pragma once

#include <spdlog/spdlog.h>

namespace faabric::util {
std::shared_ptr<spdlog::logger> getLogger(const std::string& name = "default");
}
