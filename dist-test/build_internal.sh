#!/bin/bash

set -e

export PROJ_ROOT=$(dirname $(dirname $(readlink -f $0)))
pushd ${PROJ_ROOT} >> /dev/null

# Run the debug build
inv dev.cmake --build=Debug
inv dev.cc faabric_dist_tests
inv dev.cc faabric_dist_test_server

popd >> /dev/null
