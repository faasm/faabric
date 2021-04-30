#include <faabric/util/logging.h>

#include <faabric/mpi-native/MpiExecutor.h>
#include <faabric/mpi/mpi.h>

int main(int argc, char** argv)
{
    return faabric::mpi_native::mpiNativeMain(argc, argv);
}

namespace faabric::mpi_native {
int mpiFunc()
{
    auto logger = faabric::util::getLogger();
    logger->info("Hello world from Faabric MPI Main!");

    MPI_Init(NULL, NULL);

    int rank, worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    logger->info("Hello faabric from process {} of {}", rank + 1, worldSize);

    MPI_Finalize();

    return 0;
}
}
