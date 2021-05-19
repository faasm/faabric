# Developing Faabric

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
