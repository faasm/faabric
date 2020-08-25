# Faabric ![Faabric tests](https://github.com/Shillaker/faabric/workflows/Tests/badge.svg)

Faabric provides collective communication and state to build distributed 
applications from serverless functions. 

## Build

### Dependencies

The following dependencies cannot be installed with `apt`:

- gRPC and Protobuf - https://grpc.io/docs/languages/cpp/quickstart/ 
- RapidJSON - https://rapidjson.org/
- spdlog - https://github.com/gabime/spdlog
- catch (for testing) - https://github.com/catchorg/Catch2 

See the [base Dockerfile](docker/base.dockerfile) for all the dependencies.

### CMake

Use of Clang and Ninja is recommended.

```
mkdir build
cd build
cmake -DCMAKE_C_COMPILER=clang \
  -DCMAKE_CXX_COMPILER=clang++ \
  -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  ..
ninja
```
