#include <algorithm>
#include <iostream>
#include <numeric>
#include <ranges>
#include <string>
#include <vector>

#include "mpi_bench.hpp"

template<class T>
void init_buf(std::vector<T>& x, std::vector<T>& y, T rank)
{
    std::ranges::fill(x, rank);
    std::ranges::fill(y, 0);
}

static int all_reduce_int(int x)
{
    int y = 0;
    MPI_Allreduce(&x, &y, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    return y;
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

    const int tot = std::accumulate(sizes.begin(), sizes.end(), 0);
    int64_t workload = 4 * (worldSize - 1) * sizeof(T) * tot;

    PRN_IF(rank == 0,
           "%s(np=%d) took %.4fs, total workload: %s, rate: %s",
           __func__,
           worldSize,
           d.count(),
           S(show_size(workload)),
           S(show_rate(workload, d)));
    return 0;
}

std::string safe_get_env(const char* name)
{
    if (const char* p = std::getenv(name); p) {
        return std::string(p);
    }
    return "";
}

int bench_allreduce()
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

    const std::string payload_size = safe_get_env("PAYLOAD");
    const bool only_small = all_reduce_int(payload_size == "small");
    const bool only_large = all_reduce_int(payload_size == "large");
    const bool both = !only_small && !only_large;

    std::string name(__func__);
    const char* p = std::getenv("EXP_NAME");
    if (p) {
        name += " ";
        name += p;
    }

    int ret = 0;
    PRN_IF(rank == 0, "BGN %s %s %s", S(h40), S(name), S(h40));
    if (both || only_small) {
        for (int i = 0; i < 10; ++i) {
            ret = bench_allreduce(rank, worldSize, small_sizes());
        }
    }
    if (both || only_large) {
        for (int i = 0; i < 10; ++i) {
            ret = bench_allreduce(rank, worldSize, resnet50_grad_sizes());
        }
    }

    MPI_Finalize();

    PRN_IF(rank == 0, "END %s %s %s", S(h40), S(name), S(h40));
    return ret;
}

namespace tests::mpi {
int bench_allreduce()
{
    return ::bench_allreduce();
}
} // namespace tests::mpi
