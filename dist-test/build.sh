#!/bin/bash

set -e

export PROJ_ROOT=$(dirname $(dirname $(readlink -f $0)))
pushd ${PROJ_ROOT} >> /dev/null

export CONAN_CACHE_MOUNT_SOURCE=$HOME/.conan/
# Ensure the cache directory exists before starting the containers
mkdir -p ${CONAN_CACHE_MOUNT_SOURCE}

# Run the build
docker-compose \
    run \
    --rm \
    cli \
    /code/faabric/dist-test/build_internal.sh

popd >> /dev/null
