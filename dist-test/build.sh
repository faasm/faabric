#!/bin/bash

set -e

export PROJ_ROOT=$(dirname $(dirname $(readlink -f $0)))
pushd ${PROJ_ROOT} >> /dev/null

# Run the build
export FAABRIC_DEPLOYMENT_TYPE=gha-ci
docker compose \
    run \
    --rm \
    cli \
    /code/faabric/dist-test/build_internal.sh

popd >> /dev/null
