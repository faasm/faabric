# Native MPI execution in Faabric

Faabric supports linking MPI binaries against our custom MPI implementation
used in [Faasm](https://github.com/faasm/faasm). This way, you can test the
compliance of your MPI application with our API (a subset of the standard)
without the burden of cross-compiling to WebAssembly.

To run native MPI applications you need to first modify your binary matching
the examples provided, and then build the worker image running:
```
inv container.build-mpi-native
```

Then you may run arbitrary deployments setting the right values in
`mpi-native/mpi-native.env` and running `./mpi-native/run.sh`.

You may remove all stopped and running container images with:
```bash
./mpi-native/clean.sh
```
