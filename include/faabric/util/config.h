#pragma once

#include <string>

#define MPI_HOST_STATE_LEN 20

#define DEFAULT_TIMEOUT 60000
#define RESULT_KEY_EXPIRY 30000
#define STATUS_KEY_EXPIRY 300000

namespace faabric::util {
class SystemConfig
{

  public:
    // System
    std::string serialisation;
    std::string logLevel;
    std::string logFile;
    std::string stateMode;
    std::string deltaSnapshotEncoding;

    // Redis
    std::string redisStateHost;
    std::string redisQueueHost;
    std::string redisPort;

    // Scheduling
    int noScheduler;
    int overrideCpuCount;
    std::string noTopologyHints;
    int noSingleHostOptimisations;

    // Worker-related timeouts
    int globalMessageTimeout;
    int boundTimeout;
    int schedulerReaperInterval;

    // MPI
    int defaultMpiWorldSize;
    int mpiBasePort;

    // Endpoint
    std::string endpointInterface;
    std::string endpointHost;
    int endpointPort;
    int endpointNumThreads;

    // Transport
    int functionServerThreads;
    int stateServerThreads;
    int snapshotServerThreads;
    int pointToPointServerThreads;

    // Dirty tracking
    std::string dirtyTrackingMode;
    std::string diffingMode;

    SystemConfig();

    void print();

    void reset();

  private:
    int getSystemConfIntParam(const char* name, const char* defaultValue);

    void initialise();
};

SystemConfig& getSystemConfig();
}
