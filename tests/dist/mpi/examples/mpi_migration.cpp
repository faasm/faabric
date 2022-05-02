#include <faabric/mpi/mpi.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/logging.h>

#include "mpi/mpi_native.h"

namespace tests::mpi {

static void setLocalSlots(int newNumLocalSlots)
{
    SPDLOG_INFO("Overwriting local slots from migration function");
    faabric::HostResources res;
    res.set_slots(newNumLocalSlots);
    faabric::scheduler::getScheduler().setThisHostResources(res);
}

// Outer wrapper, and re-entry point after migration
int migration(int nLoops)
{
    bool mustCheck = nLoops == NUM_MIGRATION_LOOPS;

    // Initialisation
    int res = MPI_Init(NULL, NULL);
    if (res != MPI_SUCCESS) {
        printf("Failed on MPI init\n");
        return 1;
    }

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    // After initialisation, update the local slots so that a migration
    // opportunity appears.
    if (rank == 0) {
        setLocalSlots(worldSize);
    }

    for (int i = 0; i < nLoops; i++) {
        // Make sure everyone is in sync (including those ranks that have been
        // migrated)
        MPI_Barrier(MPI_COMM_WORLD);

        tests::mpi::doAllToAll(rank, worldSize, i);

        if (mustCheck && (i % CHECK_EVERY == 0) && (i / CHECK_EVERY > 0)) {
            mustCheck = false;
            if (rank == 0) {
                SPDLOG_INFO(
                  "Checking for migrations at iteration {}/{}", i, nLoops);
            }
            // Migration point, which may or may not resume the
            // benchmark on another host for the remaining iterations.
            // This would eventually be MPI_Barrier
            MPI_Barrier(MPI_COMM_WORLD);
            mpiMigrationPoint(nLoops - i - 1);
        }
    }

    SPDLOG_DEBUG("Rank {} exitting the migration loop", rank);
    MPI_Barrier(MPI_COMM_WORLD);

    // Shutdown
    MPI_Finalize();

    return MPI_SUCCESS;
}
}
