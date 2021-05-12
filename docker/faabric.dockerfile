FROM ubuntu:20.04
ARG FAABRIC_VERSION

# Flag to say we're in a container
ENV FAABRIC_DOCKER="on"

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

# Put the code in place
WORKDIR /code
RUN git clone -b v${FAABRIC_VERSION} https://github.com/faasm/faabric

WORKDIR /code/faabric
RUN pip3 install -r requirements.txt

# Static build
RUN inv dev.cmake
RUN inv dev.cc faabric
RUN inv dev.cc faabric_tests

# Shared build
RUN inv dev.cmake --shared
RUN inv dev.cc faabric --shared
RUN inv dev.install faabric --shared

# CLI setup
ENV TERM xterm-256color
SHELL ["/bin/bash", "-c"]

RUN echo ". /code/faabric/bin/workon.sh" >> ~/.bashrc
CMD ["/bin/bash", "-l"]
