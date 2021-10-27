# Developing Faabric

## Development environment

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

Most external dependencies are installed through CMake in
[cmake/ExternalProjects.cmake](./cmake/ExternalProjects.cmake).
The remaining installed packages can be inspected in the [`faabric-base`](
./docker/faabric-base.dockerfile) dockerfile.

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

## Testing

We have some standard tests using [Catch2](https://github.com/catchorg/Catch2)
under the `faabric_tests` target.

## Distributed tests

The distributed tests are aimed at testing distributed features across more than
one host.

### Running locally

To set up the distributed tests locally:

```bash
# Start the CLI
./bin/cli.sh

# Build both the tests and the server
inv dev.cc faabric_dist_tests
inv dev.cc faabric_dist_test_server
```

In another terminal, start the server:

```bash
./dist-tests/dev_server.sh
```

Back in the CLI, you can then run the tests:

```bash
faabric_dist_tests
```

You can repeat this process of rebuilding, restarting the server, and running.

### Running as if in CI

To run the distributed tests as if in CI:

```bash
# Clean up
docker-compose stop

# Build and run
./dist-test/build.sh
./dist-test/run.sh
```

## Releasing

Create a new branch, then find and replace the current version with the relevant
bumped version. It should appear in:

- `VERSION`
- `.env`
- `.github/workflows/tests.yml`.
- `mpi-native/mpi-native.env`

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

