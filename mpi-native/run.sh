#!/bin/bash

set -e

THIS_DIR=$(dirname $(readlink -f $0))
PROJ_ROOT=${THIS_DIR}/..

check_exit_code () {
    ARR=$(docker-compose \
            --file ${COMPOSE_FILE} \
            --env-file ${ENV_FILE} \
            ps -aq)

    RET_CODES=$(docker inspect $ARR --format='{{.State.ExitCode}}')

    for code in $RET_CODES
    do
        if [ "$code" != "0" ]; then
            exit 1
        fi
    done
}

pushd ${PROJ_ROOT} >> /dev/null

source ./mpi-native/mpi-native.env

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

check_exit_code

exit 0
