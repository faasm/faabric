# Faabric [![Faabric tests](https://github.com/faasm/faabric/workflows/Tests/badge.svg?branch=master)](https://github.com/faasm/faabric/actions) [![License](https://img.shields.io/github/license/faasm/faabric.svg)](https://github.com/faasm/faabric/blob/master/LICENSE.md) 

Faabric is a messaging and state layer for serverless applications.

## Building and Development

You can build Faabric natively or using the containerised environment.

### Containerised (recommended)

Run the following:

```bash
./bin/cli.sh
```

This container has everything you need, and the script will also mount your 
current checkout of the code. This means you can just run the following:

```bash
# Set up the build
inv dev.cmake

# Build the tests
inv dev.cc faabric_tests

# Run the tests
faabric_tests
```

To stop the `faabric`-related containers run:
```bash
docker-compose down
```

### Native

The only external dependency _not_ installed through CMake is `gRPC` which
should be installed according to the instructions
[here](https://grpc.io/docs/languages/cpp/quickstart/).

Use of Clang and Ninja is recommended. From the root of this project you can
run:

```bash
mkdir build
cd build

cmake \
  -GNinja \
  -DCMAKE_C_COMPILER=clang \
  -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_BUILD_TYPE=Release \
  ..

ninja
```

You can also run the CLI with:

```bash
# Set up env
export FAABRIC_BUILD_DIR=<some dir>
source bin/workon.sh

# Install requirements
pip install -r requirements.txt

# Build Faabric
inv dev.cc faabric
```

## Releasing

Create a new branch, then find and replace the current version with the relevant 
bumped version. Currently it's held in `VERSION`, `.env`,
`mpi-native/mpi-native.env` and the Github Actions configuration.

Once done, commit and push, then run:

```bash
source bin/workon.sh
inv git.tag
```

This will trigger the release build in Github Actions which will build all the
containers. Once that's complete, create a PR from your branch and make sure the
tests pass as normal.

If you want to overwrite a tag, you can run:

```bash
inv git.tag --force
```

### Building images manually

Containers are built with Github Actions, so you should only need to build them
yourself when diagnosing issues.

To build the main container, run:

```bash
source bin/workon.sh

# Build
inv container.build

# Push
inv container.push

# Build and push
inv container.build --push
```

## Additional documentation

More detail on some key features and implementations can be found below:

- [Native MPI builds](docs/mpi_native.md) - run native applications against
Faabric's MPI library.
