namespace tests::mpi {
int bench_send_recv();
}

int main(int argc, char* argv[])
{
    tests::mpi::bench_send_recv();
    return 0;
}
