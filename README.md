# Faabric ![Faabric tests](https://github.com/faasm/faabric/workflows/Tests/badge.svg) ![License](https://img.shields.io/github/license/faasm/faabric.svg)

Faabric provides collective communication and state to build distributed 
applications from serverless functions. 

## Build

### Dependencies

The only external dependency _not_ installed through CMake is `gRPC` which
should be installed according to the instructions
[here](https://grpc.io/docs/languages/cpp/quickstart/).

### CMake

Use of Clang and Ninja is recommended.

```bash
mkdir build
cd build
cmake -DCMAKE_C_COMPILER=clang \
  -DCMAKE_CXX_COMPILER=clang++ \
  -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  ..
ninja
```

## CLI

To set up the CLI:

```bash
source workon.sh
pip install -r requirements.txt
```

## Docker

To build the Docker Faabric Docker containers, run:

```bash
# Build locally
inv container.build

# Push
inv container.push

# Build locally and push (requires permissions)
inv container.build --push
```

