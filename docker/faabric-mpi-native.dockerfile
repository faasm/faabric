ARG FAABRIC_VERSION
FROM faasm/faabric:${FAABRIC_VERSION}

WORKDIR /code/faabric

# Build MPI native lib
RUN inv dev.cmake --shared
RUN inv dev.cc faabricmpi_native --shared
RUN inv dev.install faabricmpi_native --shared

# Build examples
RUN inv mpi-native.build-mpi
