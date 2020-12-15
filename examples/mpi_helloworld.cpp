#include <faabric/mpi/mpi.h>
#include <faabric/util/logging.h>

int main()
{
    auto logger = faabric::util::getLogger();

    MPI_Init(NULL, NULL);

    int rank, worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    logger->info("Hello world from rank %i of %i", rank, worldSize);

    MPI_Finalize();

    return 0;
}
