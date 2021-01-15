# Native MPI execution in Faabric

Faabric supports linking MPI binaries against our custom MPI implementation
used in [Faasm](https://github.com/faasm/faasm). This way, you can test the
compliance of your MPI application with our API (a subset of the standard)
without the burden of cross-compiling to WebAssembly.

To run native MPI applications you need to: compile the dynamic library, and
slightly modify the original soource code.

## Compiling the library

Compilation should be smooth if you are running our recommended containerised
[development environment](../README.md). You may access the container running
`./bin/cli.sh`.

Then, to compile the library:
```bash
inv dev.cmake --shared
inv dev.cc faabricmpi_native --shared
inv dev.install faabricmpi_native --shared
```

## Adapting the source code

## Running the binary

To run an example, run this command _outside_ the container:
```bash
# The --clean flag re-creates _all_ containers
inv mpi.execute mpi_helloworld --clean --np 5
```

To clean the cluster and set the development one again:
```bash
inv mpi.clean
```

Using the `--force` flag will recreate _all_ containers hence finishing any
sessions you may have open:
```bash
inv mpi.clean --force
```

## Debugging

If at some point you reach an unstable state of the cluster, stop it completely
using:
```bash
docker-compose down
```
