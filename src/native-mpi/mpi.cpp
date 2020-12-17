#include <faabric/mpi/mpi.h>
#include <faabric/util/logging.h>

int MPI_Init(int* argc, char*** argv)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Init");

    return MPI_SUCCESS;
}

int MPI_Comm_rank(MPI_Comm comm, int* rank)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Comm_rank");

    *rank = 1337;

    return MPI_SUCCESS;
}

int MPI_Comm_size(MPI_Comm comm, int* size)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Comm_size");

    *size = 9337;

    return MPI_SUCCESS;
}

int MPI_Finalize()
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Finalize");

    return MPI_SUCCESS;
}
