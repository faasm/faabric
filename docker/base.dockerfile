FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install -y software-properties-common

RUN sudo apt install -y \
   build-essential \
   autoconf \
   libtool \
   pkg-config \
   ninja

# Latest cmake
RUN apt remove --purge --auto-remove cmake
WORKDIR /setup
RUN wget -q -O cmake-linux.sh https://github.com/Kitware/CMake/releases/download/v3.18.2/cmake-3.18.2-Linux-x86_64.sh
RUN sh cmake-linux.sh -- --skip-license
RUN rm cmake-linux.sh

# gRPC, protobuf etc.
RUN git clone --recurse-submodules -b v1.31.0 https://github.com/grpc/grpc
WORKDIR /setup/grpc/cmake/build
RUN cmake -GNinja \
    -DgRPC_INSTALL=ON \
    -DgRPC_BUILD_TESTS=OFF \
    ../..
RUN ninja install

# Tidy up
RUN apt-get clean autoclean
RUN apt-get autoremove

WORKDIR /
CMD /bin/bash