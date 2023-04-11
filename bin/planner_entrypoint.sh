#!/bin/bash

set -e

# The planner needs to support being used both from faabric and from faasm,
# and in both cases it must support mounting the built binaries to update it.
# Thus, we add a wrapper around the entrypoint command that takes as an input
# the binary dir, and waits until the binary file exists

BINARY_DIR=${$1:-/build/faabric/static/bin}
BINARY_FILE=${BINARY_DIR}/planner_server

until test -f ${BINARY_FILE}
do
    echo "Waiting for planner server binary to be available at: ${BINARY_FILE}"
    sleep 3
done

# Once the binary file is available, run it
${BINARY_FILE}
