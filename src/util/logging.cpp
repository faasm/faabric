#include <faabric/util/config.h>
#include <faabric/util/logging.h>

#include "spdlog/sinks/basic_file_sink.h"
#include <spdlog/sinks/stdout_sinks.h>
//#include "spdlog/sinks/stdout_color_sinks.h"

namespace faabric::util {
static std::shared_ptr<spdlog::logger> defaultLogger;
static std::shared_ptr<spdlog::logger> mpiLogger;
static std::unordered_map<std::string, std::shared_ptr<spdlog::logger>> loggers;

/*
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
    loggers["default"] = spdlog::stderr_color_mt("console");

    // Work out log level from environment
    SystemConfig& conf = faabric::util::getSystemConfig();
    if (conf.logLevel == "debug") {
        loggers["default"]->set_level(spdlog::level::debug);
    } else if (conf.logLevel == "trace") {
        loggers["default"]->set_level(spdlog::level::trace);
    } else if (conf.logLevel == "off") {
        loggers["default"]->set_level(spdlog::level::off);
    } else {
        loggers["default"]->set_level(spdlog::level::info);
    }
}
*/

static void initLogging(const std::string& name)
{
    SystemConfig& conf = faabric::util::getSystemConfig();
    
    // Configure the sink list. By default all loggers _at least_ log to
    // the console
    try {
        std::vector<spdlog::sink_ptr> sinks;
        // Issue with color in multiple sinks requires this trick
        // https://github.com/gabime/spdlog/issues/290#issuecomment-250716701
        //auto stdout_sink = spdlog::sinks::stdout_sink_mt::instance();
        //auto color_sink = std::make_shared<spdlog::sinks::ansicolor_sink>(stdout_sink);
        auto stdout_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();
        sinks.push_back(stdout_sink);

        // If log level is set to file add an additional file sink
        if (conf.logFile == "on") {
            sinks.push_back(std::make_shared<spdlog::sinks::basic_file_sink_mt>(
              fmt::format("/var/log/faabric/{}.log", name)));
        }

        // Initialize the logger and set level
        loggers[name] =
          std::make_shared<spdlog::logger>(name, sinks.begin(), sinks.end());
        if (conf.logLevel == "debug") {
            loggers[name]->set_level(spdlog::level::debug);
        } else if (conf.logLevel == "trace") {
            loggers[name]->set_level(spdlog::level::trace);
        } else if (conf.logLevel == "off") {
            loggers[name]->set_level(spdlog::level::off);
        } else {
            loggers[name]->set_level(spdlog::level::info);
        }

        // Add custom pattern. See here the formatting options:
        // https://github.com/gabime/spdlog/wiki/3.-Custom-formatting#pattern-flags
        // <timestamp> [log-level] [logger-name] [rank] <message>
        loggers[name]->set_pattern("%D %T [%n] (%l) %v");
    } catch (const spdlog::spdlog_ex& e) {
        throw std::runtime_error(fmt::format("Error initializing {} logger: {}", name, e.what()));
    }
}

std::shared_ptr<spdlog::logger> getLogger(const std::string& name)
{
    // Lazy-initialize logger
    // TODO consider using loggers.contains(<key>) when C++20 support
    if (loggers.count(name) == 0) {
        initLogging(name);
    }

    return loggers[name];
    /*
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
    */
}
}
