#include <numeric>

#include "mpi_bench.hpp"

template<class T>
void init_buf(std::vector<T>& x, std::vector<T>& y, T rank)
{
    std::ranges::fill(x, rank);
    std::ranges::fill(y, 0);
}

static int bench_send_recv(int rank, int worldSize, std::vector<int> sizes = {})
{
    using T = int;
    auto dt = MPI_INT;
    constexpr int from = 0;
    constexpr int to = 1;

    const int workload = std::accumulate(sizes.begin(), sizes.end(), 0);
    auto t0 = CLOCK::now();

    for (int n : sizes) {
        std::vector<T> x(n);
        std::vector<T> y(n);
        init_buf(x, y, rank);

        const int tag = 0;
        if (rank == from) {
            MPI_Send(x.data(), n, dt, to, tag, MPI_COMM_WORLD);
        } else if (rank == to) {
            MPI_Status status;
            MPI_Recv(y.data(), n, dt, from, tag, MPI_COMM_WORLD, &status);
        }
    }

    auto t1 = CLOCK::now();
    duration d = t1 - t0;

    PRN_IF(rank == 0,
           "%s(np=%d) took %.4fs, total workload: %s, rate: %s",
           __func__,
           worldSize,
           d.count(),
           S(show_size(workload)),
           S(show_rate(workload, d)));

    return 0;
}

int bench_send_recv()
{
    const std::string h40(40, '=');
    MPI_Init(NULL, NULL);
    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    char version[1024];
    int verlen;
    MPI_Get_library_version(version, &verlen);
    PRN_IF(rank == 0, "%s: version[%d]: %s", __func__, verlen, version);

    std::string name(__func__);

    PRN_IF(rank == 0, "BGN %s %s %s", S(h40), S(name), S(h40));

    int ret = 0;
    for (int i = 0; i < 10; ++i) {
        ret = bench_send_recv(rank, worldSize, small_sizes());
    }
    for (int i = 0; i < 10; ++i) {
        ret = bench_send_recv(rank, worldSize, resnet50_grad_sizes());
    }

    PRN_IF(rank == 0, "END %s %s %s", S(h40), S(name), S(h40));
    MPI_Finalize();
    return ret;
}

namespace tests::mpi {
int bench_send_recv()
{
    return ::bench_send_recv();
}
} // namespace tests::mpi
