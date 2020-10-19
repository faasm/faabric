FROM faasm/grpc-root:0.0.5
ARG FAABRIC_VERSION

# Redis
RUN apt install -y redis-tools

# Put the code in place
WORKDIR /code
RUN git clone -b v${FAABRIC_VERSION} https://github.com/faasm/faabric

# Build the code
WORKDIR /build/faabric
RUN cmake \
    -GNinja \
    -DCMAKE_CXX_COMPILER=/usr/bin/clang++-10 \
    -DCMAKE_C_COMPILER=/usr/bin/clang-10 \
    -DCMAKE_BUILD_TYPE=Release \
    /code/faabric

RUN ninja faabric
RUN ninja faabric_tests

CMD /bin/bash
