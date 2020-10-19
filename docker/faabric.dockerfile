FROM faasm/grpc-root:0.0.5

# Redis
RUN apt install -y redis-tools

# Build the code
WORKDIR /code/faabric
COPY . .
WORKDIR /code/faabric/build
RUN cmake \
    -GNinja \
    -DCMAKE_CXX_COMPILER=/usr/bin/clang++-10 \
    -DCMAKE_C_COMPILER=/usr/bin/clang-10 \
    -DCMAKE_BUILD_TYPE=Release \
    ..
RUN ninja faabric_tests 

CMD /bin/bash
