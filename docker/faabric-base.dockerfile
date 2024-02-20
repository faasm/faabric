FROM ubuntu:22.04

# Configure APT repositories
ARG LLVM_VERSION_MAJOR
RUN apt update \
    && apt upgrade -y \
    && apt install -y \
        curl \
        gpg \
        software-properties-common \
        wget \
    # LLVM APT Repo config
    && wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key | tee /etc/apt/trusted.gpg.d/apt.llvm.org.asc \
    && add-apt-repository -y "deb http://apt.llvm.org/jammy/ llvm-toolchain-jammy-${LLVM_VERSION_MAJOR} main"

# Install APT packages
RUN apt update && apt install -y \
    autoconf \
    automake \
    build-essential \
    clang-${LLVM_VERSION_MAJOR} \
    clang-format-${LLVM_VERSION_MAJOR} \
    clang-tidy-${LLVM_VERSION_MAJOR} \
    clang-tools-${LLVM_VERSION_MAJOR} \
    doxygen \
    g++-12 \
    git \
    libboost-filesystem-dev \
    libc++-${LLVM_VERSION_MAJOR}-dev \
    libc++abi-${LLVM_VERSION_MAJOR}-dev \
    libcurl4-openssl-dev \
    libhiredis-dev \
    libpolly-${LLVM_VERSION_MAJOR}-dev \
    libpython3-dev \
    libssl-dev \
    libstdc++-12-dev \
    libtool \
    libunwind-${LLVM_VERSION_MAJOR}-dev \
    libz-dev \
    lld-${LLVM_VERSION_MAJOR} \
    lldb-${LLVM_VERSION_MAJOR} \
    make \
    ninja-build \
    pkg-config \
    python3-dev \
    python3-pip \
    python3-venv \
    redis-tools \
    sudo \
    unzip

# Install up-to-date CMake
RUN apt remove --purge --auto-remove cmake \
    && mkdir -p /setup \
    && cd /setup \
    && wget -q -O cmake-linux.sh \
        https://github.com/Kitware/CMake/releases/download/v3.28.0/cmake-3.28.0-linux-x86_64.sh \
    && sh cmake-linux.sh -- --skip-license --prefix=/usr/local \
    && apt clean autoclean -y \
    && apt autoremove -y

# Install Conan
RUN curl -s -L -o \
        /tmp/conan-latest.deb https://github.com/conan-io/conan/releases/download/1.63.0/conan-ubuntu-64.deb \
    && sudo dpkg -i /tmp/conan-latest.deb \
    && rm -f /tmp/conan-latest.deb

# Tidy up
RUN apt clean autoclean -y \
    && apt autoremove -y
