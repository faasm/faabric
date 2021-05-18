#!/bin/bash

set -e

THIS_DIR=$(dirname $(readlink -f $0))
export PROJ_ROOT=${THIS_DIR}/..
pushd $PROJ_ROOT >> /dev/null

# Run the build
inv dev.cmake
inv dev.cc faabric_dist_tests
inv dev.cc faabric_dist_test_server

# Copy the results
cp -r /build/faabric/static/* /build/faabric/dist-test/

popd >> /dev/null
