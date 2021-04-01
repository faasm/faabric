FROM ubuntu:20.04

RUN apt-get update
RUN apt-get install -y software-properties-common

RUN apt install -y \
   autoconf \
   build-essential \
   clang-10 \
   clang-format-10 \
   git \
   libhiredis-dev \
   libtool \
   libboost-filesystem-dev \
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

# -------------------------------------------------------------------
# gRPC, protobuf, flatbuffers
#
# NOTE - the versions of these must be carefully chosen. 
# At the time of writing, this PR was the most up-to-date:
# https://github.com/google/flatbuffers/pull/6338
# 
# This meant gRPC version 1.36.1 and flatbuffers at this specific commit: 
# https://github.com/google/flatbuffers/commit/276b1bc342d23142e4b2b9b9fadbf076474deec9
# -------------------------------------------------------------------

# Static libs
RUN git clone --recurse-submodules -b v1.36.1 https://github.com/grpc/grpc
WORKDIR /setup/grpc/cmake/build-static
RUN cmake -GNinja \
    -DgRPC_INSTALL=ON \
    -DBUILD_SHARED_LIBS=OFF \
    -DgRPC_BUILD_TESTS=OFF \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    ../..
RUN ninja install

# Shared libs
WORKDIR /setup/grpc/cmake/build-shared
RUN cmake -GNinja \
    -DgRPC_INSTALL=ON \
    -DBUILD_SHARED_LIBS=ON \
    -DgRPC_BUILD_TESTS=OFF \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    ../..
RUN ninja install

# Flatbuffers
WORKDIR /setup
RUN git clone --recurse-submodules -b v1.12.0 https://github.com/google/flatbuffers
WORKDIR /setup/flatbuffers/cmake/build-static
RUN cmake -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    ../..
RUN ninja install

WORKDIR /setup/flatbuffers/cmake/build-shared
RUN cmake -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DFLATBUFFERS_BUILD_SHAREDLIB=ON \
    ../..
RUN ninja install

# Tidy up
WORKDIR /
RUN rm -r /setup
RUN apt-get clean autoclean
RUN apt-get autoremove
