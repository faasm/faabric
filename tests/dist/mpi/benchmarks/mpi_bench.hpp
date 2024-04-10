#pragma once
#include <chrono>
#include <string>
#include <vector>

#ifdef USE_REAL_MPI
#include <mpi.h>
#else
#include "mpi/mpi_native.h"
#include <faabric/mpi/mpi.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

int MPI_Get_library_version(char* p, int* n);

#endif

#define PRN(fmt, ...) std::fprintf(stdout, fmt "\n", __VA_ARGS__)

#define PRN_IF(cond, fmt, ...)                                                 \
    if (cond) {                                                                \
        PRN(fmt, __VA_ARGS__);                                                 \
    }

#define S(x) (x).c_str()

using CLOCK = std::chrono::high_resolution_clock;
using duration = std::chrono::duration<double>;

std::vector<int> small_sizes();

std::vector<int> resnet50_grad_sizes();

std::string show_size(int64_t workload);

std::string show_rate(int64_t workload, duration d);
