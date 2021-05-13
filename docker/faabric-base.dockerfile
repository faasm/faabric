FROM ubuntu:20.04

RUN apt-get update
RUN apt-get install -y software-properties-common

RUN apt install -y \
    autoconf \
    build-essential \
    clang-10 \
    clang-format-10 \
    clang-tidy-10 \
    git \
    libhiredis-dev \
    libtool \
    libboost-filesystem-dev \
    libpython3-dev \
    ninja-build \
    pkg-config \
    python3-dev \
    python3-pip \
    python3-venv \
    redis-tools \
    wget

# Latest cmake
RUN apt remove --purge --auto-remove cmake
WORKDIR /setup
RUN wget -q -O \
    cmake-linux.sh \
    https://github.com/Kitware/CMake/releases/download/v3.18.2/cmake-3.18.2-Linux-x86_64.sh
RUN sh cmake-linux.sh -- --skip-license --prefix=/usr/local

# Tidy up
WORKDIR /
RUN rm -r /setup
RUN apt-get clean autoclean
RUN apt-get autoremove
