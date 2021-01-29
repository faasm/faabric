#pragma once

#include <spdlog/spdlog.h>

namespace faabric::util {
void initMpiLogging();

void initLogging();

std::shared_ptr<spdlog::logger> getLogger(const std::string& name = "default");
}
