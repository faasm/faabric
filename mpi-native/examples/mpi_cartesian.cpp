#include <cmath>
#include <faabric/mpi/mpi.h>
#include <faabric/util/compare.h>
#include <stdio.h>

#include <faabric/mpi-native/MpiExecutor.h>
#include <faabric/util/logging.h>
int main(int argc, char** argv)
{
    auto logger = faabric::util::getLogger();
    auto& scheduler = faabric::scheduler::getScheduler();
    auto& conf = faabric::util::getSystemConfig();

    // Global configuration
    conf.maxNodes = 1;
    conf.maxNodesPerFunction = 1;

    bool __isRoot;
    int __worldSize;
    if (argc < 2) {
        logger->debug("Non-root process started");
        __isRoot = false;
    } else if (argc < 3) {
        logger->error("Root process started without specifying world size!");
        return 1;
    } else {
        logger->debug("Root process started");
        __worldSize = std::stoi(argv[2]);
        __isRoot = true;
        logger->debug("MPI World Size: {}", __worldSize);
    }

    // Pre-load message to bootstrap execution
    if (__isRoot) {
        faabric::Message msg = faabric::util::messageFactory("mpi", "exec");
        msg.set_mpiworldsize(__worldSize);
        scheduler.callFunction(msg);
    }

    {
        faabric::executor::SingletonPool p;
        p.startPool();
    }

    return 0;
}

int faabric::executor::mpiFunc()
{
    MPI_Init(NULL, NULL);

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    // Prepare arguments
    int ndims = 2;
    int sideLength = static_cast<int>(std::floor(std::sqrt(worldSize)));
    int nprocs = sideLength * sideLength;
    int dims[2] = { sideLength, sideLength };
    int periods[2] = { 0, 0 };
    int reorder = 0;
    MPI_Comm cart;

    // Test MPI_Cart_create
    if (MPI_Cart_create(MPI_COMM_WORLD, ndims, dims, periods, reorder, &cart) !=
        MPI_SUCCESS) {
        printf("MPI_Cart_init failed!\n");
        return 1;
    }
    printf("MPI_Cart_create succesful check.\n");

    // Test MPI_Cart_get
    int coords[2];
    int expected[2];
    if (rank >= nprocs) {
        expected[0] = expected[1] = MPI_UNDEFINED;
    } else {
        expected[0] = rank / sideLength;
        expected[1] = rank % sideLength;
    }
    if (MPI_Cart_get(cart, ndims, dims, periods, coords) != MPI_SUCCESS) {
        printf("MPI_Cart_get failed!\n");
        return 1;
    }

    // Test integrity of results
    if (!faabric::util::compareArrays<int>(coords, expected, 2)) {
        printf("Wrong cartesian coordinates.\n");
        return 1;
    }
    printf("MPI_Cart_get succesful check.\n");

    // Now check that the original rank can be retrieved.
    if (rank < nprocs) {
        int nRank;
        if (MPI_Cart_rank(cart, coords, &nRank) != MPI_SUCCESS) {
            printf("MPI_Cart_rank failed!\n");
            return 1;
        }
        if (rank != nRank) {
            printf(
              "Wrong rank from cartesian coordinates. Expected: %i Got: %i",
              rank,
              nRank);
            return 1;
        }
        printf("MPI_Cart_rank succesful check.\n");
    }

    printf("MPI Cartesian Topology Checks Succesful.\n");
    MPI_Finalize();

    return 0;
}
