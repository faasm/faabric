#!/bin/bash

set -e

THIS_DIR=$(dirname $(readlink -f $0))
PROJ_ROOT=${THIS_DIR}/..

pushd ${PROJ_ROOT} > /dev/null

echo "Running Faabric CLI (${FAABRIC_CLI_IMAGE})"

INNER_SHELL=${SHELL:-"/bin/bash"}

# Make sure the CLI is running already in the background (avoids creating a new
# container every time)
docker compose \
    up \
    --no-recreate \
    -d \
    cli

# Attach to the CLI container
docker compose \
    exec \
    cli \
    ${INNER_SHELL}

popd > /dev/null
