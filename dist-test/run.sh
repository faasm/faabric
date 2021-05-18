#!/bin/bash

set -e

THIS_DIR=$(dirname $(readlink -f $0))
export PROJ_ROOT=${THIS_DIR}/..

if [ -z "$1" ]; then
    echo "Must provide a mode, ci or local"
    exit 1
elif [ "$1" == "ci" ]; then
    echo "Running in CI mode"
    MODE="ci"
elif [ "$1" == "local" ]; then
    echo "Running in local mode"
    MODE="local"
else
    echo "Unrecognised mode: $1"
    exit 1
fi

pushd ${PROJ_ROOT} >> /dev/null

# Prepare the CLI image name
if [[ -z "${FAABRIC_CLI_IMAGE}" ]]; then
    VERSION=$(cat VERSION)
    export FAABRIC_CLI_IMAGE=faasm/faabric:${VERSION}
fi

pushd dist-test >> /dev/null

if [ "$MODE" == "local" ]; then
    INNER_SHELL=${SHELL:-"/bin/bash"}

    # Start everything in the background
    docker-compose \
        up \
        --no-recreate \
        -d \
        master

    # Run the CLI
    docker-compose \
        exec \
        master \
        ${INNER_SHELL}
else
    # Run the tests directly
    docker-compose \
        run \
        master \
        /build/faabric/static/bin/faabric_dist_tests

    docker-compose \
        stop
fi

popd >> /dev/null
popd >> /dev/null
