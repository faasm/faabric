#include <faabric/mpi/mpi.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

#include "mpi/mpi_native.h"

#include <chrono>
#include <thread>

namespace tests::mpi {

int allToAllAndSleep()
{
    MPI_Init(NULL, NULL);

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    // Call barrier and all-to-all multiple times
    int numBarriers = 500;
    for (int i = 0; i < numBarriers; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        doAllToAll(rank, worldSize, i);
    }

    int timeToSleepSec = 5;
    SPDLOG_INFO("Rank {} going to sleep for {} seconds", rank, timeToSleepSec);
    SLEEP_MS(timeToSleepSec * 1000);
    SPDLOG_INFO("Rank {} waking up", rank);

    for (int i = 0; i < numBarriers; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        doAllToAll(rank, worldSize, i);
    }

    MPI_Finalize();

    return 0;
}
}
