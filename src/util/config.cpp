#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <faabric/util/network.h>

namespace faabric::util {
SystemConfig& getSystemConfig()
{
    static SystemConfig conf;
    return conf;
}

SystemConfig::SystemConfig()
{
    this->initialise();
}

void SystemConfig::initialise()
{
    // System
    serialisation = getEnvVar("SERIALISATION", "json");
    logLevel = getEnvVar("LOG_LEVEL", "info");
    logFile = getEnvVar("LOG_FILE", "off");
    stateMode = getEnvVar("STATE_MODE", "inmemory");
    deltaSnapshotEncoding =
      getEnvVar("DELTA_SNAPSHOT_ENCODING", "pages=4096;xor;zstd=1");

    // Redis
    redisStateHost = getEnvVar("REDIS_STATE_HOST", "localhost");
    redisQueueHost = getEnvVar("REDIS_QUEUE_HOST", "localhost");
    redisPort = getEnvVar("REDIS_PORT", "6379");

    // Scheduling
    noScheduler = this->getSystemConfIntParam("NO_SCHEDULER", "0");
    overrideCpuCount = this->getSystemConfIntParam("OVERRIDE_CPU_COUNT", "0");

    // Worker-related timeouts (all in seconds)
    globalMessageTimeout =
      this->getSystemConfIntParam("GLOBAL_MESSAGE_TIMEOUT", "60000");
    boundTimeout = this->getSystemConfIntParam("BOUND_TIMEOUT", "30000");

    // MPI
    defaultMpiWorldSize =
      this->getSystemConfIntParam("DEFAULT_MPI_WORLD_SIZE", "5");

    // Endpoint
    endpointInterface = getEnvVar("ENDPOINT_INTERFACE", "");
    endpointHost = getEnvVar("ENDPOINT_HOST", "");
    endpointPort = this->getSystemConfIntParam("ENDPOINT_PORT", "8080");
    endpointNumThreads =
      this->getSystemConfIntParam("ENDPOINT_NUM_THREADS", "4");

    if (endpointHost.empty()) {
        // Get the IP for this host
        endpointHost =
          faabric::util::getPrimaryIPForThisHost(endpointInterface);
    }
}

int SystemConfig::getSystemConfIntParam(const char* name,
                                        const char* defaultValue)
{
    int value = stoi(getEnvVar(name, defaultValue));

    return value;
};

void SystemConfig::reset()
{
    this->initialise();
}

void SystemConfig::print()
{
    const std::shared_ptr<spdlog::logger>& logger = getLogger();

    logger->info("--- System ---");
    logger->info("SERIALISATION              {}", serialisation);
    logger->info("LOG_LEVEL                  {}", logLevel);
    logger->info("LOG_FILE                   {}", logFile);
    logger->info("STATE_MODE                 {}", stateMode);
    logger->info("DELTA_SNAPSHOT_ENCODING    {}", deltaSnapshotEncoding);

    logger->info("--- Redis ---");
    logger->info("REDIS_STATE_HOST           {}", redisStateHost);
    logger->info("REDIS_QUEUE_HOST           {}", redisQueueHost);
    logger->info("REDIS_PORT                 {}", redisPort);

    logger->info("--- Scheduling ---");
    logger->info("NO_SCHEDULER               {}", noScheduler);
    logger->info("OVERRIDE_CPU_COUNT         {}", overrideCpuCount);

    logger->info("--- Timeouts ---");
    logger->info("GLOBAL_MESSAGE_TIMEOUT     {}", globalMessageTimeout);
    logger->info("BOUND_TIMEOUT              {}", boundTimeout);

    logger->info("--- MPI ---");
    logger->info("DEFAULT_MPI_WORLD_SIZE  {}", defaultMpiWorldSize);

    logger->info("--- Endpoint ---");
    logger->info("ENDPOINT_INTERFACE         {}", endpointInterface);
    logger->info("ENDPOINT_HOST              {}", endpointHost);
    logger->info("ENDPOINT_PORT              {}", endpointPort);
    logger->info("ENDPOINT_NUM_THREADS       {}", endpointNumThreads);
}
}
