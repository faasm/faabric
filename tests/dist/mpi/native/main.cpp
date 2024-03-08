namespace tests::mpi {
int bench_allreduce();
}

int main(int argc, char* argv[])
{
    tests::mpi::bench_allreduce();
    return 0;
}

/*
docker run --user mpirun --rm -it --entrypoint bash faasm.azurecr.io/openmpi-worker:0.13.1
*/
