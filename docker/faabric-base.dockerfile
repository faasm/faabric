FROM ubuntu:22.04

# Configure APT repositories
RUN apt update \
    && apt upgrade -y \
    && apt install -y \
        curl \
        gpg \
        software-properties-common \
        wget \
    # apt-key add is now deprecated for security reasons, we add individual
    # keys manually.
    # https://askubuntu.com/questions/1286545/what-commands-exactly-should-replace-the-deprecated-apt-key
    && wget -O /tmp/llvm-snapshot.gpg.key \
        https://apt.llvm.org/llvm-snapshot.gpg.key \
    && gpg --no-default-keyring \
        --keyring /tmp/tmp-key.gpg \
        --import /tmp/llvm-snapshot.gpg.key \
    && gpg --no-default-keyring \
        --keyring /tmp/tmp-key.gpg \
        --export --output /etc/apt/keyrings/llvm-snapshot.gpg \
    && rm /tmp/tmp-key.gpg \
    && echo \
        "deb [signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/focal/ llvm-toolchain-focal-10 main" \
        >> /etc/apt/sources.list.d/archive_uri-http_apt_llvm_org_focal_-jammy.list \
    && wget -O /tmp/kitware-archive-latest.asc \
        https://apt.kitware.com/keys/kitware-archive-latest.asc \
    && gpg --no-default-keyring \
        --keyring /tmp/tmp-key.gpg \
        --import /tmp/kitware-archive-latest.asc \
    && gpg --no-default-keyring \
        --keyring /tmp/tmp-key.gpg \
        --export --output /etc/apt/keyrings/kitware-archive-latest.gpg \
    && rm /tmp/tmp-key.gpg \
    && echo \
        "deb [signed-by=/etc/apt/keyrings/kitware-archive-latest.gpg] https://apt.kitware.com/ubuntu/ jammy main" \
        >> /etc/apt/sources.list.d/archive_uri-https_apt_kitware_com_ubuntu_jammy_-jammy.list

# Install APT packages
RUN apt update && apt install -y \
    autoconf \
    automake \
    clang-10 \
    clang-13 \
    clang-format-10 \
    clang-format-13 \
    clang-tidy-10 \
    clang-tidy-13 \
    clang-tools-13 \
    cmake \
    doxygen \
    g++-11 \
    git \
    kitware-archive-keyring \
    libboost-filesystem-dev \
    libc++-13-dev \
    libc++abi-13-dev \
    libcurl4-openssl-dev \
    libhiredis-dev \
    libpython3-dev \
    libssl-dev \
    libstdc++-11-dev \
    libtool \
    libunwind-13-dev \
    libz-dev \
    lld-13 \
    lldb-13 \
    make \
    ninja-build \
    pkg-config \
    python3-dev \
    python3-pip \
    python3-venv \
    redis-tools \
    sudo \
    unzip

# Install Conan
RUN curl -s -L -o \
        /tmp/conan-latest.deb https://github.com/conan-io/conan/releases/download/1.52.0/conan-ubuntu-64.deb \
    && sudo dpkg -i /tmp/conan-latest.deb \
    && rm -f /tmp/conan-latest.deb

# Tidy up
RUN apt clean autoclean && apt autoremove
