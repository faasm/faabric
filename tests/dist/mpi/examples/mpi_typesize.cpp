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

namespace tests::mpi {

int typeSize()
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
}
