#!/bin/bash

set -e

export PROJ_ROOT=$(dirname $(dirname $(readlink -f $0)))
pushd ${PROJ_ROOT}/dist-test >> /dev/null

# Set up image name
if [[ -z "${FAABRIC_CLI_IMAGE}" ]]; then
    VERSION=$(cat ../VERSION)
    export FAABRIC_CLI_IMAGE=faasm/faabric:${VERSION}
fi

# Run the build
docker-compose \
    run \
    builder \
    /code/faabric/dist-test/build_internal.sh

popd >> /dev/null
