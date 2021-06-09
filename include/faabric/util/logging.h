#pragma once

// See spdlog source for definition of levels, e.g. SPDLOG_LEVEL_TRACE=0,
// SPDLOG_LEVEL_DEBUG=1
// https://github.com/gabime/spdlog/blob/v1.x/include/spdlog/common.h

// Note that defining SPDLOG_ACTIVE_LEVEL sets the *minimum available* log
// level, however, we must also programmatically set the logging level using
// spdlog::set_level (or it will default to info).
//
// Defining this minimum level is to noop debug and trace logging statements
// in a release build for performance reasons.

#ifdef NDEBUG
// Allow info and up in release build
#define SPDLOG_ACTIVE_LEVEL 2
#else
// Allow all levels in debug build
#define SPDLOG_ACTIVE_LEVEL 0
#endif

#include <spdlog/spdlog.h>

namespace faabric::util {
void initLogging();
}
