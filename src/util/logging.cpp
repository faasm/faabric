#include <faabric/util/config.h>
#include <faabric/util/logging.h>

#include "spdlog/sinks/basic_file_sink.h"
#include <spdlog/sinks/stdout_color_sinks.h>

namespace faabric::util {
static std::shared_ptr<spdlog::logger> defaultLogger;
static std::shared_ptr<spdlog::logger> mpiLogger;
// static bool isInitialised = false;

void initMpiLogging(int rank)
{
    // To set up the MPI logging, we must wait until we know our rank.
    // Logging to both console + file or only file is supported.
    if (mpiLogger != nullptr) {
        return;
    }

    faabric::util::getLogger()->debug("Initialising MPI logger");
    SystemConfig& conf = faabric::util::getSystemConfig();
    if (conf.mpiLogLevel == "on") {
        try {
            // Initialise the logger with two sinks
            auto console_sink =
              std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
            console_sink->set_level(spdlog::level::debug);
            console_sink->set_pattern(fmt::format("[MPI - Rank: {}] %v", rank));

            auto file_sink =
              std::make_shared<spdlog::sinks::basic_file_sink_mt>(
                "logs/multisink.txt", true);
            file_sink->set_level(spdlog::level::debug);
            file_sink->set_pattern(fmt::format("[MPI - Rank: {}] %v", rank));

            spdlog::sinks_init_list sink_list = { file_sink, console_sink };
            mpiLogger = std::make_shared<spdlog::logger>(
              "mpi_logger", sink_list.begin(), sink_list.end());
            mpiLogger->set_level(spdlog::level::debug);
        } catch (const spdlog::spdlog_ex& e) {
            faabric::util::getLogger()->error(
              "MPI Logger initialization failed: {}", e.what());
        }
    } else if (conf.mpiLogLevel == "file") {
        try {
            mpiLogger = spdlog::basic_logger_mt("mpi_logger", "logs/test.txt");
            mpiLogger->set_level(spdlog::level::debug);
            mpiLogger->set_pattern(fmt::format("[MPI - Rank: {}] %v", rank));
        } catch (const spdlog::spdlog_ex& e) {
            faabric::util::getLogger()->error(
              "MPI logger initialization failed: {}", e.what());
        }
    } else {
        faabric::util::getLogger()->warn(
          "MPI logging has not been enabled in the system's config.");
        faabric::util::getLogger()->warn("Reverting to the default logger.");
        mpiLogger = defaultLogger;
    }
}

void initLogging()
{
    if (defaultLogger != nullptr) {
        return;
    }

    // Initialise the default logger
    defaultLogger = spdlog::stderr_color_mt("console");

    // Work out log level from environment
    SystemConfig& conf = faabric::util::getSystemConfig();
    if (conf.logLevel == "debug") {
        defaultLogger->set_level(spdlog::level::debug);
    } else if (conf.logLevel == "trace") {
        defaultLogger->set_level(spdlog::level::trace);
    } else if (conf.logLevel == "off") {
        defaultLogger->set_level(spdlog::level::off);
    } else {
        defaultLogger->set_level(spdlog::level::info);
    }

    // isInitialised = true;
}

std::shared_ptr<spdlog::logger> getLogger(const std::string& logger)
{
    if (logger == "default") {
        if (defaultLogger == nullptr) {
            initLogging();
        }
        return defaultLogger;
    } else if (logger == "mpi") {
        if (mpiLogger == nullptr) {
            // We need to explicitly initialise the MPI logger with our rank
            throw std::runtime_error("Use of uninitialized MPI logger");
        }
        return mpiLogger;
    } else {
        throw std::runtime_error(
          fmt::format("Unrecognized logger: {}", logger));
    }
}
}
