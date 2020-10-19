FROM ubuntu:20.04
ARG FAABRIC_VERSION

RUN apt-get update
RUN apt-get install -y software-properties-common

RUN apt install -y \
   autoconf \
   clang-format-10 \
   build-essential \
   git \
   libhiredis-dev \
   libtool \
   libboost-filesystem-dev \
   libcurl4-openssl-dev \
   ninja-build \
   python3-dev \
   python3-pip \
   pkg-config \
   wget

# Python deps
RUN pip3 install black

# Latest cmake
RUN apt remove --purge --auto-remove cmake
WORKDIR /setup
RUN wget -q -O \
    cmake-linux.sh \
    https://github.com/Kitware/CMake/releases/download/v3.18.2/cmake-3.18.2-Linux-x86_64.sh

RUN sh cmake-linux.sh -- --skip-license --prefix=/usr/local

# gRPC, protobuf etc.
RUN git clone --recurse-submodules -b v1.31.0 https://github.com/grpc/grpc
WORKDIR /setup/grpc/cmake/build
RUN cmake -GNinja \
    -DgRPC_INSTALL=ON \
    -DgRPC_BUILD_TESTS=OFF \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    ../..
RUN ninja install

# Redis
RUN apt install -y redis-tools

# Build the code
WORKDIR /code
RUN git clone https://github.com/faasm/faabric
WORKDIR /code/faabric
RUN git checkout ${FAABRIC_VERSION}
WORKDIR /code/faabric/build
RUN cmake \
    -GNinja \
    -DCMAKE_CXX_COMPILER=/usr/bin/clang++-10 \
    -DCMAKE_C_COMPILER=/usr/bin/clang-10 \
    -DCMAKE_BUILD_TYPE=Release \
    ..

# Tidy up
WORKDIR /
RUN rm -r /setup
RUN apt-get clean autoclean
RUN apt-get autoremove

CMD /bin/bash
