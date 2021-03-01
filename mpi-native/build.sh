#!/bin/bash

set -e

THIS_DIR=$(dirname $(readlink -f $0))
PROJ_ROOT=${THIS_DIR}/..

pushd ${PROJ_ROOT} >> /dev/null

. ./mpi-native/mpi-native.env

docker-compose \
    --file ${COMPOSE_FILE} \
    --env-file ${ENV_FILE} \
    build

popd >> /dev/null
