#include <faabric/mpi/mpi.h>
#include <faabric/util/compare.h>
#include <stdio.h>
#include <string>

#define NUM_ELEMENT 4

#include <faabric/mpi-native/MpiExecutor.h>
#include <faabric/util/logging.h>

int main(int argc, char** argv)
{
    return faabric::mpi_native::mpiNativeMain(argc, argv);
}

namespace faabric::mpi_native {
int mpiFunc()
{
    MPI_Init(NULL, NULL);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int dataSize = sizeof(int);
    int* sharedData = new int[NUM_ELEMENT];
    int* expectedPutData = new int[NUM_ELEMENT];

    // Populate some data in shared region and not
    for (int i = 0; i < NUM_ELEMENT; i++) {
        sharedData[i] = 10 * rank + i;
        expectedPutData[i] = 10 * rank + i;
    }

    // Create a window
    MPI_Win window;
    int winSize = NUM_ELEMENT * dataSize;
    MPI_Win_create(
      sharedData, winSize, 1, MPI_INFO_NULL, MPI_COMM_WORLD, &window);

    // Check that the memory has not been corrupted (happens when pointer to
    // MPI_Win is handled wrongly)
    if (rank < 3) {
        bool putDataEqual = faabric::util::compareArrays<int>(
          sharedData, expectedPutData, NUM_ELEMENT);

        if (!putDataEqual) {
            printf("Rank %i - stack corrupted by win_create\n", rank);
            return 1;
        } else if (rank < 3) {
            printf("Rank %i - stack OK\n", rank);
        }
    }

    if (rank == 0) {
        // Check base of window
        void* actualBase;
        int baseFlag;
        MPI_Win_get_attr(window, MPI_WIN_BASE, &actualBase, &baseFlag);
        if (actualBase != sharedData || baseFlag != 1) {
            printf("MPI_WIN_BASE not as expected (%p != %p (%i))\n",
                   actualBase,
                   sharedData,
                   baseFlag);
            return 1;
        }

        // Check size of window
        int actualSize;
        baseFlag = 0;
        MPI_Win_get_attr(window, MPI_WIN_SIZE, (void*)&actualSize, &baseFlag);
        if (actualSize != winSize || baseFlag != 1) {
            printf("MPI_WIN_SIZE not as expected (%d != %d (%i))\n",
                   actualSize,
                   winSize,
                   baseFlag);
            return 1;
        }

        // Check size of window
        int actualDispUnit;
        baseFlag = 0;
        MPI_Win_get_attr(
          window, MPI_WIN_DISP_UNIT, (void*)&actualDispUnit, &baseFlag);
        if (actualDispUnit != 1 || baseFlag != 1) {
            printf("MPI_WIN_DISP_UNIT not as expected (%d != %d (%i))\n",
                   actualDispUnit,
                   1,
                   baseFlag);
            return 1;
        }

        printf("Win attr checks complete\n");
    }

    delete[] sharedData;
    delete[] expectedPutData;

    MPI_Win_free(&window);
    MPI_Finalize();

    return 0;
}
}
