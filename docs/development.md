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

### Distributed tests

The distributed tests are aimed at testing a more "realistic" distributed
environment and use multiple containers.

To set up the initial build:

```bash
# Build the relevant binaries
./dist-test/build.sh
```

Then to run and develop locally:

```bash
# Start up the CLI container
./dist-test/run.sh local

# Rebuild and run inside CLI container as usual
inv dev.cc faabric_dist_tests
/build/static/bin/faabric_dist_tests

# To rebuild the server, you'll need to rebuild and restart
inv dev.cc faabric_dist_test_server

# Outside the container
./dist-test/restart_server.sh
```

To see logs locally:

```bash
cd dist-test
docker-compose logs -f
```

To run as if in CI:

```bash
# Clean up
cd dist-test
docker-compose stop
docker-compose rm

# Run once through
./dist-test/run.sh
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

