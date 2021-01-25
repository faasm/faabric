#include <faabric/mpi/mpi.h>
#include <stdio.h>

bool checkTypeSize(MPI_Datatype dt, int expected, const char* name)
{
    int actual;
    MPI_Type_size(dt, &actual);
    if (actual != expected) {
        printf("MPI %s size not as expected (got %i, expected %i)\n",
               name,
               actual,
               expected);
        return false;
    } else {
        return true;
    }
}

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
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        if (!checkTypeSize(MPI_INT, sizeof(int), "int")) {
            return 1;
        }
        if (!checkTypeSize(MPI_LONG, sizeof(long), "long")) {
            return 1;
        }
        if (!checkTypeSize(MPI_LONG_LONG, sizeof(long long), "long long")) {
            return 1;
        }
        if (!checkTypeSize(
              MPI_LONG_LONG_INT, sizeof(long long int), "long long int")) {
            return 1;
        }
        if (!checkTypeSize(MPI_DOUBLE, sizeof(double), "double")) {
            return 1;
        }
        struct
        {
            double a;
            int b;
        } s;
        if (!checkTypeSize(MPI_DOUBLE_INT, sizeof s, "double int")) {
            return 1;
        }
        if (!checkTypeSize(MPI_FLOAT, sizeof(float), "float")) {
            return 1;
        }
        if (!checkTypeSize(MPI_DOUBLE, sizeof(double), "double")) {
            return 1;
        }
        if (!checkTypeSize(MPI_CHAR, sizeof(char), "char")) {
            return 1;
        }

        printf("MPI type sizes as expected\n");
    }

    MPI_Finalize();

    return 0;
}
