#include <faabric/mpi/mpi.h>
#include <stdio.h>

#include <faabric/util/logging.h>
#include <faabric/mpi-native/MpiExecutor.h>
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

    // Send and receive messages in a ring
    // Sendrecv prevents worrying about possible deadlocks
    int right = (rank + 1) % worldSize;
    int maxRank = worldSize - 1;
    int left = rank > 0 ? rank - 1 : maxRank;

    // Receive from the left and send to the right
    int sendValue = rank;
    int recvValue = -1;
    printf(
      "Rank: %i. Receiving from: %i - Sending to: %i\n", rank, left, right);
    MPI_Sendrecv(&sendValue,
                 1,
                 MPI_INT,
                 right,
                 0,
                 &recvValue,
                 1,
                 MPI_INT,
                 left,
                 0,
                 MPI_COMM_WORLD,
                 nullptr);

    // Check the received value is as expected
    if (recvValue != left) {
        printf("Rank %i - sendrecv not working properly (got %i expected %i)\n",
               rank,
               recvValue,
               left);
        return 1;
    }
    printf("Rank %i - sendrecv working properly\n", rank);

    MPI_Finalize();

    return 0;
}
