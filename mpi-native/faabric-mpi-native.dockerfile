FROM faasm/grpc-root:0.0.16
ARG FAABRIC_VERSION

# Note - the version of grpc-root here can be quite behind as it's rebuilt very
# rarely

# Redis
RUN apt install -y \
    libpython3-dev \
    python3-dev \
    python3-pip \
    python3-venv \
    redis-tools

# Put the code in place
WORKDIR /code
# RUN git clone -b v${FAABRIC_VERSION} https://github.com/faasm/faabric
RUN git clone -b standalone-mpi https://github.com/csegarragonz/faabric

WORKDIR /code/faabric

RUN pip3 install invoke

# Build MPI native lib
RUN inv dev.cmake --shared
RUN inv dev.cc faabricmpi_native --shared
RUN inv dev.install faabricmpi_native --shared

# Build examples
RUN inv examples.build
