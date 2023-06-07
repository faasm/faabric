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
inv tests
```

To stop the `faabric`-related containers run:

```bash
docker compose down
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

## Debugging

Note that Faabric provides its own `.gdbinit` file which will ensure segfaults
(used in dirty tracking) aren't caught by gdb by default.

## Testing

We have some standard tests using [Catch2](https://github.com/catchorg/Catch2)
under the `faabric_tests` target.

We add a wrapper script around the tests target to set the right environment
variables, and capture idiomatic ways to call the tests. You can use the
wrapper scripts in the following ways:

```bash
# Run the whole test suite
inv tests [--debug]

# Run just one test case
inv tests --test-case "Test JSON contains required keys" [--repeats <num>]

# Run all test cases defined in one file
inv tests --test-file test_json

# Run all test cases defined in one directory
inv tests --test-dir util
```

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

In another terminal, (re)start the server:

```bash
# Start
./dist-test/dev_server.sh

# Restart
./dist-test/dev_server.sh restart
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
docker compose stop

# Build and run
./dist-test/build.sh
./dist-test/run.sh
```

## Creating a new tag

Create a new branch, then bump the code version:

```bash
inv git.bump
```

This will increment the minor version, to bump the code to an arbitrary version
you can run:

```bash
inv git.bump --ver=<new_version>
```

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

After the new tag has been merged in, and in order to keep a clean commit
history, you may re-tag the code again:

```bash
inv git.tag --force
```

### Building images manually

Containers are built with Github Actions, when a new tag is pushed, so you
should only need to build them yourself when diagnosing issues.

To build the main container, run:

```bash
source bin/workon.sh

# Build
inv docker.build -c faabric [--push]
```

The base container is not re-built often, and not re-built as part of Github
Actions. If you ever need to add an APT dependency, or update the Conan
version, run:

```bash
source bin/workon.sh

inv docker.build -c faabric-base [--push]
```

## Publishing a release

To publish a release in Github, make sure you are in the main branch, and have
just tagged the code (see previous section).

Then, you can create a release on [Github](https://github.com/faasm/faabric/releases)
and publish it from the command line. If it is the first time you are creating
a release you will have to configure a Github access token (see below).

First, generate a draft release:

```bash
inv git.release_create
```

Then, after verifying that the release looks fine, you may publish it:

```bash
inv git.release_publish
```

### Configuring a Github access token

Follow the instructions on [how to create a personal access token](
https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token).
Then, create a config file for faabric in the main directory named
`faabric.ini` with the following contents:

```toml
[Github]
access_token = <paste your personal access token>
```
