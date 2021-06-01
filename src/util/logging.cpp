#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace faabric::util {

std::shared_mutex loggerMx;
static std::unordered_map<std::string, std::shared_ptr<spdlog::logger>> loggers;

static void initLogging(const std::string& name)
{
    SystemConfig& conf = faabric::util::getSystemConfig();

    // Configure the sink list. By default all loggers _at least_ log to
    // the console
    try {
        std::vector<spdlog::sink_ptr> sinks;
        auto stdout_sink =
          std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        sinks.push_back(stdout_sink);

        // If file logging is set, add an additional file sink
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
        // <timestamp> [logger-name] (log-level) <message>
        loggers[name]->set_pattern("%^%D %T [%n] (%l)%$ %v");
    } catch (const spdlog::spdlog_ex& e) {
        throw std::runtime_error(
          fmt::format("Error initializing {} logger: {}", name, e.what()));
    }
}

std::shared_ptr<spdlog::logger> getLogger(const std::string& name)
{
    // Lazy-initialize logger
    if (loggers.find(name) == loggers.end()) {
        faabric::util::FullLock lock(loggerMx);
        if (loggers.find(name) == loggers.end()) {
            initLogging(name);
        }
    }

    {
        faabric::util::SharedLock lock(loggerMx);
        return loggers[name];
    }
}
}
