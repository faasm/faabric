#!/bin/bash

set -e

THIS_DIR=$(dirname $(readlink -f $0))
export PROJ_ROOT=${THIS_DIR}/..

pushd ${PROJ_ROOT} >> /dev/null

VERSION=$(cat VERSION)
export FAABRIC_CLI_IMAGE=faasm/faabric:${VERSION}

pushd dist-test >> /dev/null

docker-compose \
    run \
    worker \
    /code/faabric/dist-test/build_internal.sh

popd >> /dev/null
popd >> /dev/null
