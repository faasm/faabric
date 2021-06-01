#!/bin/bash

export PROJ_ROOT=$(dirname $(dirname $(readlink -f $0)))
pushd ${PROJ_ROOT}/dist-test >> /dev/null

# Set up image name
if [[ -z "${FAABRIC_CLI_IMAGE}" ]]; then
    VERSION=$(cat ../VERSION)
    export FAABRIC_CLI_IMAGE=faasm/faabric:${VERSION}
fi

RETURN_VAL=0

if [ "$1" == "local" ]; then
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
    RETURN_VAL=$?

    echo "-------------------------------------------"
    echo "                WORKER LOGS                "
    echo "-------------------------------------------"
    docker-compose logs worker

    docker-compose stop
fi

popd >> /dev/null

exit $RETURN_VAL
