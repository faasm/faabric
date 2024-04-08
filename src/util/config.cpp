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
    // TODO(planner-scheduler): remove some of this
    overrideCpuCount = this->getSystemConfIntParam("OVERRIDE_CPU_COUNT", "0");
    overrideFreeCpuStart =
      this->getSystemConfIntParam("OVERRIDE_FREE_CPU_START", "0");
    batchSchedulerMode = getEnvVar("BATCH_SCHEDULER_MODE", "bin-pack");

    // Worker-related timeouts (all in seconds)
    globalMessageTimeout =
      this->getSystemConfIntParam("GLOBAL_MESSAGE_TIMEOUT", "60000");
    boundTimeout = this->getSystemConfIntParam("BOUND_TIMEOUT", "30000");
    reaperIntervalSeconds =
      this->getSystemConfIntParam("REAPER_INTERVAL_SECS", "30");

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

    // Transport
    functionServerThreads =
      this->getSystemConfIntParam("FUNCTION_SERVER_THREADS", "2");
    stateServerThreads =
      this->getSystemConfIntParam("STATE_SERVER_THREADS", "2");
    snapshotServerThreads =
      this->getSystemConfIntParam("SNAPSHOT_SERVER_THREADS", "2");
    // FIXME: temporarily set this value to a higher number to work-around:
    // https://github.com/faasm/faabric/issues/335
    pointToPointServerThreads =
      this->getSystemConfIntParam("POINT_TO_POINT_SERVER_THREADS", "8");

    // Dirty tracking
    dirtyTrackingMode = getEnvVar("DIRTY_TRACKING_MODE", "segfault");
    diffingMode = getEnvVar("DIFFING_MODE", "xor");

    // Planner
    plannerHost = getEnvVar("PLANNER_HOST", "planner");
    plannerPort = this->getSystemConfIntParam("PLANNER_PORT", "8080");
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

    SPDLOG_INFO("--- System ---");
    SPDLOG_INFO("SERIALISATION              {}", serialisation);
    SPDLOG_INFO("LOG_LEVEL                  {}", logLevel);
    SPDLOG_INFO("LOG_FILE                   {}", logFile);
    SPDLOG_INFO("STATE_MODE                 {}", stateMode);
    SPDLOG_INFO("DELTA_SNAPSHOT_ENCODING    {}", deltaSnapshotEncoding);

    SPDLOG_INFO("--- Redis ---");
    SPDLOG_INFO("REDIS_STATE_HOST           {}", redisStateHost);
    SPDLOG_INFO("REDIS_QUEUE_HOST           {}", redisQueueHost);
    SPDLOG_INFO("REDIS_PORT                 {}", redisPort);

    SPDLOG_INFO("--- Scheduling ---");
    SPDLOG_INFO("OVERRIDE_CPU_COUNT         {}", overrideCpuCount);

    SPDLOG_INFO("--- Timeouts ---");
    SPDLOG_INFO("GLOBAL_MESSAGE_TIMEOUT     {}", globalMessageTimeout);
    SPDLOG_INFO("BOUND_TIMEOUT              {}", boundTimeout);

    SPDLOG_INFO("--- MPI ---");
    SPDLOG_INFO("DEFAULT_MPI_WORLD_SIZE  {}", defaultMpiWorldSize);

    SPDLOG_INFO("--- Endpoint ---");
    SPDLOG_INFO("ENDPOINT_INTERFACE         {}", endpointInterface);
    SPDLOG_INFO("ENDPOINT_HOST              {}", endpointHost);
    SPDLOG_INFO("ENDPOINT_PORT              {}", endpointPort);
    SPDLOG_INFO("ENDPOINT_NUM_THREADS       {}", endpointNumThreads);
}
}
