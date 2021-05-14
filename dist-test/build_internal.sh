#!/bin/bash

SRC_DIR=/build/faabric/static/bin
DEST_DIR=/build/faabric-copy/bin

set -e

THIS_DIR=$(dirname $(readlink -f $0))
export PROJ_ROOT=${THIS_DIR}/..

pushd $PROJ_ROOT >> /dev/null

# Run the build
inv dev.cmake
inv dev.cc faabric_dist_tests
inv dev.cc faabric_dist_test_server

# Copy into the mounted directory
echo "Copying build into mounted directory"
cp -r ${SRC_DIR}/* ${DEST_DIR}/

popd >> /dev/null
