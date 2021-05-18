# Developing Faabric

## Testing

We have some standard tests using [Catch2](https://github.com/catchorg/Catch2)
under the `faabric_tests` target.

### Distributed tests

The distributed tests are aimed at testing a more "realistic" distributed
environment and use multiple containers.

To use:

```
# Build the relevant binaries
./dist-test/build.sh

# Run locally
./dist-test/run.sh local
/build/static/bin/faabric_dist_tests

# Rebuild inside local set-up with usual commands, e.g.
inv dev.cc faabric_dist_tests

# Clean up
cd dist-test
docker-compose stop
docker-compose rm

# Run as if in CI
./dist-test/run.sh
```
