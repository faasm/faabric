#!/bin/bash

set -e
set -x

THIS_DIR=$(dirname $(readlink -f $0))
PROJ_ROOT=${THIS_DIR}/..

if [ -z "$1" ]; then
    echo "Must provide a mode, ci or local"
    exit 1
elif [ "$1" == "ci" ]; then
    echo "Running in CI mode"
elif [ "$1" == "local" ]; then
    echo "Running in local mode"
else
    echo "Unrecognised mode: $1"
    exit 1
fi


pushd ${PROJ_ROOT}>>/dev/null

docker-compose -f \
    dist-test/docker-compose.yml \
    run \
    master \
    /code/faabric/dist-test/master.sh
\

popd >> /dev/null
