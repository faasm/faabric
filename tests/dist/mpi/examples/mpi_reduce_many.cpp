#include <faabric/mpi/mpi.h>
#include <stdio.h>
#include <string.h>

namespace tests::mpi {

int reduceMany()
{
    MPI_Init(NULL, NULL);

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    int root = 0;
    int* expected = nullptr;
    int* result = nullptr;

    int numReduce = 100;
    if (rank == root) {
        expected = new int[3];
        result = new int[3];

        for (int r = 0; r < numReduce; r++) {
            // Build expectation
            memset(expected, 0, 3 * sizeof(int));
            memset(result, 0, 3 * sizeof(int));

            for (int r = 0; r < worldSize; r++) {
                expected[0] += r;
                expected[1] += 10 * r;
                expected[2] += 100 * r;
            }

            // Create a buffer for the result, and put the values for the
            // root in straight away
            result[0] = root;
            result[1] = 10 * root;
            result[2] = 100 * root;

            // Call the reduce in place on the root
            MPI_Reduce(
              MPI_IN_PLACE, result, 3, MPI_INT, MPI_SUM, root, MPI_COMM_WORLD);

            // Check vs. expectation
            for (int i = 0; i < 3; i++) {
                if (result[i] != expected[i]) {
                    printf("Reduce failed\n");
                    return 1;
                }
            }
        }

        printf("Reduce as expected\n");

        delete[] expected;
        delete[] result;
    } else {
        // Create an array of three numbers specific to this rank
        int numsThisProc[3] = { rank, 10 * rank, 100 * rank };
        for (int r = 0; r < numReduce; r++) {
            MPI_Reduce(
              numsThisProc, result, 3, MPI_INT, MPI_SUM, root, MPI_COMM_WORLD);
        }
    }

    MPI_Finalize();

    return MPI_SUCCESS;
}
}
