namespace tests::mpi {
int bench_allreduce();
}

int main(int argc, char* argv[])
{
    tests::mpi::bench_allreduce();
    return 0;
}
