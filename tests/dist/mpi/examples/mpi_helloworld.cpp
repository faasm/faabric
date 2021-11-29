#include <faabric/mpi/mpi.h>
#include <faabric/util/logging.h>

namespace tests::mpi {

int helloWorld()
{
    SPDLOG_INFO("Hello world from Faabric MPI Main!");

    MPI_Init(NULL, NULL);

    int rank, worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    SPDLOG_INFO("Hello faabric from process {} of {}", rank + 1, worldSize);

    MPI_Finalize();

    return 0;
}
}
