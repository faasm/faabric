#include "mpi_bench.hpp"

#include <algorithm>

#ifndef USE_REAL_MPI

int MPI_Get_library_version(char* p, int* n)
{
    static const char* version = "faabric-mpi-?";
    *n = strlen(version);
    memcpy(p, version, *n);
    p[*n] = '\0';
    return 0;
}

#endif

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

constexpr int64_t Mi = 1 << 20;
constexpr int64_t Gi = 1 << 30;

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
