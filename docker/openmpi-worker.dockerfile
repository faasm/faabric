FROM faasm.azurecr.io/openmpi:0.4.6
WORKDIR /code/faabric
COPY ./tests/dist/mpi /code/faabric/tests/dist/mpi
WORKDIR /code/faabric/tests/dist/mpi/native
RUN mkdir build && cd build && cmake .. && cmake --build .
