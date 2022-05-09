# Native MPI execution in Faabric

Faabric supports linking MPI binaries against our custom MPI implementation
used in [Faasm](https://github.com/faasm/faasm). This way, you can test the
compliance of your MPI application with our API (a subset of the standard)
without the burden of cross-compiling to WebAssembly.

To run native MPI applications, you can check the examples in the distributed
tests (`tests/dist/mpi/examples`). If you need to implement a new method, check
first how is it done in [faasm](https://github.com/faasm/faasm/blob/main/src/wavm/mpi.cpp).

To run the distributed test set for MPI, follow the instructions to set up the
distributed tests in the [development docs](https://github.com/faasm/faabric/blob/main/docs/source/development.md)
and use the tag `[mpi]`, i.e.:

```bash
faabric_dist_tests [mpi]
```
