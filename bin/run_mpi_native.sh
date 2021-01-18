#!/bin/bash

set -e

THIS_DIR=$(dirname $(readlink -f $0))
PROJ_ROOT=${THIS_DIR}/..

pushd ${PROJ_ROOT} >> /dev/null

source ./docker/mpi-native.env

# Check for command line arguments
docker-compose \
    --file ${COMPOSE_FILE} \
    --env-file ${ENV_FILE} \
    up -d \
    --scale worker=$((${MPI_WORLD_SIZE} -1)) \
    --force-recreate

docker-compose \
    --file ${COMPOSE_FILE} \
    --env-file ${ENV_FILE} \
    logs master worker

popd >> /dev/null
