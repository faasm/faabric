#include "mpi_bench.hpp"

int bench_send_recv()
{
    MPI_Init(NULL, NULL);
    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    MPI_Finalize();
    return 0;
}

namespace tests::mpi {
int bench_send_recv()
{
    return ::bench_send_recv();
}
} // namespace tests::mpi
