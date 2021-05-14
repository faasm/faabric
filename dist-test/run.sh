#!/bin/bash

set -e

THIS_DIR=$(dirname $(readlink -f $0))
export PROJ_ROOT=${THIS_DIR}/..

if [ -z "$1" ]; then
    echo "Must provide a mode, ci or local"
    exit 1
elif [ "$1" == "ci" ]; then
    echo "Running in CI mode"

    export FAABRIC_LOCAL_BUILD_DIR=${PROJ_ROOT}/build
    export FAABRIC_BUILD_MOUNT=/build/local-build
elif [ "$1" == "local" ]; then
    echo "Running in local mode"

    # Assume this is running as part of the Faasm dev set-up
    export FAABRIC_LOCAL_BUILD_DIR=${PROJ_ROOT}/../dev/faabric/build/static
    export FAABRIC_BUILD_MOUNT=/build/faabric/static

    if [ ! -d "$FAABRIC_LOCAL_BUILD_DIR" ]; then
        echo "Unable to find local build dir: $FAABRIC_LOCAL_BUILD_DIR"
        exit 1
    fi
else
    echo "Unrecognised mode: $1"
    exit 1
fi

pushd ${PROJ_ROOT} >> /dev/null

# Prepare the default version
VERSION=$(cat VERSION)
export FAABRIC_CLI_IMAGE=faasm/faabric:${VERSION}

pushd dist-test >> /dev/null

docker-compose \
    run \
    master \
    /code/faabric/dist-test/master.sh

popd >> /dev/null
popd >> /dev/null
