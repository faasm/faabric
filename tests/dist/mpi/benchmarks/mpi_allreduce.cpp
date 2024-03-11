#include <algorithm>
#include <chrono>
#include <iostream>
#include <numeric>
#include <ranges>
#include <string>
#include <vector>

#ifdef USE_REAL_MPI
#include <mpi.h>
#else
#include "mpi/mpi_native.h"
#include <faabric/mpi/mpi.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#endif

using CLOCK = std::chrono::high_resolution_clock;
using duration = std::chrono::duration<double>;

constexpr int64_t Mi = 1 << 20;
constexpr int64_t Gi = 1 << 30;

std::vector<int> small_sizes()
{
    std::vector<int> x(1000);
    std::ranges::fill(x, 8);
    return x;
}

std::vector<int> resnet50_grad_sizes()
{
    return {
        1000,    2048000, 2048, 2048, 2048,    1048576, 512,  512,
        512,     2359296, 512,  512,  512,     1048576, 2048, 2048,
        2048,    1048576, 512,  512,  512,     2359296, 512,  512,
        512,     1048576, 2048, 2048, 2048,    2048,    2048, 2048,
        1048576, 512,     512,  512,  2097152, 2359296, 512,  512,
        512,     524288,  1024, 1024, 1024,    262144,  256,  256,
        256,     589824,  256,  256,  256,     262144,  1024, 1024,
        1024,    262144,  256,  256,  256,     589824,  256,  256,
        256,     262144,  1024, 1024, 1024,    262144,  256,  256,
        256,     589824,  256,  256,  256,     262144,  1024, 1024,
        1024,    262144,  256,  256,  256,     589824,  256,  256,
        256,     262144,  1024, 1024, 1024,    262144,  256,  256,
        256,     589824,  256,  256,  256,     262144,  1024, 1024,
        1024,    1024,    1024, 1024, 262144,  524288,  256,  256,
        256,     589824,  256,  256,  256,     131072,  512,  512,
        512,     65536,   128,  128,  128,     147456,  128,  128,
        128,     65536,   512,  512,  512,     65536,   128,  128,
        128,     147456,  128,  128,  128,     65536,   512,  512,
        512,     65536,   128,  128,  128,     147456,  128,  128,
        128,     65536,   512,  512,  512,     512,     512,  512,
        65536,   131072,  128,  128,  128,     147456,  128,  128,
        128,     32768,   256,  256,  256,     16384,   64,   64,
        64,      36864,   64,   64,   64,      16384,   256,  256,
        256,     16384,   64,   64,   64,      36864,   64,   64,
        64,      16384,   256,  256,  256,     256,     256,  256,
        16384,   16384,   64,   64,   64,      36864,   64,   64,
        64,      4096,    64,   64,   64,      9408,
    };
}

std::string show_size(int64_t workload)
{
    char buf[64];
    if (workload > Gi) {
        double unit = static_cast<double>(Gi);
        sprintf(buf, "%.3f%s", workload / unit, "GiB");
    } else if (workload > Mi) {
        double unit = static_cast<double>(Mi);
        sprintf(buf, "%.3f%s", workload / unit, "MiB");
    } else {
        sprintf(buf, "%ld%s", workload, "B");
    }
    return buf;
}

std::string show_rate(int64_t workload, duration d)
{
    double unit = static_cast<double>(Gi);
    double r = static_cast<double>(workload) / unit / d.count();
    char buf[64];
    sprintf(buf, "%.3f%s/s", r, "GiB");
    return buf;
}

template<class T>
void init_buf(std::vector<T>& x, std::vector<T>& y, T rank)
{
    std::ranges::fill(x, rank);
    std::ranges::fill(y, 0);
}

static int bench_allreduce(int rank, int worldSize, std::vector<int> sizes = {})
{
    using T = int;
    auto dt = MPI_INT;

    auto t0 = CLOCK::now();
    for (int n : sizes) {
        std::vector<T> x(n);
        std::vector<T> y(n);
        init_buf(x, y, rank);
        MPI_Allreduce(x.data(), y.data(), n, dt, MPI_SUM, MPI_COMM_WORLD);
    }
    auto t1 = CLOCK::now();
    duration d = t1 - t0;

    int tot = std::accumulate(sizes.begin(), sizes.end(), 0);
    int64_t workload = 4 * (worldSize - 1) * sizeof(T) * tot;

    if (rank == 0) {
        std::fprintf(stdout,
                     "%s(np=%d) took %.4fs, total workload: %s, rate: %s\n",
                     __func__,
                     worldSize,
                     d.count(),
                     show_size(workload).c_str(),
                     show_rate(workload, d).c_str());
    }
    return 0;
}

int bench_allreduce()
{
    const std::string h40(40, '=');

    MPI_Init(NULL, NULL);

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    std::string name(__func__);
    const char* p = std::getenv("EXP_NAME");
    if (p) {
        name += " ";
        name += p;
    }
    if (rank == 0) {
        std::fprintf(stdout,
                     "%s %s %s %s\n",
                     "BGN",
                     h40.c_str(),
                     name.c_str(),
                     h40.c_str());
    }

    int ret = 0;

    for (int i = 0; i < 10; ++i) {
        ret = bench_allreduce(rank, worldSize, small_sizes());
    }
    for (int i = 0; i < 10; ++i) {
        ret = bench_allreduce(rank, worldSize, resnet50_grad_sizes());
    }

    MPI_Finalize();

    if (rank == 0) {
        std::fprintf(stdout,
                     "%s %s %s %s\n",
                     "END",
                     h40.c_str(),
                     name.c_str(),
                     h40.c_str());
    }
    return ret;
}

namespace tests::mpi {
int bench_allreduce()
{
    return ::bench_allreduce();
}
} // namespace tests::mpi
