#include <faabric/mpi/mpi.h>
#include <faabric/util/logging.h>

#include <unistd.h>

int main()
{
    faabric::util::initLogging();
    auto logger = faabric::util::getLogger();

    MPI_Init(NULL, NULL);

    int rank, worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    sleep(2);
    logger->info("Hello world from rank {} of {}", rank, worldSize);

    MPI_Finalize();

    return 0;
}
