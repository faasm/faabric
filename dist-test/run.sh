#!/bin/bash

export PROJ_ROOT=$(dirname $(dirname $(readlink -f $0)))
pushd ${PROJ_ROOT} >> /dev/null

export CONAN_CACHE_MOUNT_SOURCE=$HOME/.conan/
RETURN_VAL=0

export OVERRIDE_CPU_COUNT=4

# Run the test server in the background
docker-compose \
    up \
    -d \
    dist-test-server

# Run the tests directly
docker-compose \
    run \
    --rm \
    cli \
    /build/faabric/static/bin/faabric_dist_tests

RETURN_VAL=$?

echo "-------------------------------------------"
echo "                SERVER LOGS                "
echo "-------------------------------------------"
docker-compose logs dist-test-server

# Stop everything
docker-compose stop

popd >> /dev/null

exit $RETURN_VAL
