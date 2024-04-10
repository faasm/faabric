#!/bin/bash

set -e

export PROJ_ROOT=$(dirname $(dirname $(readlink -f $0)))
pushd ${PROJ_ROOT} >> /dev/null

# Activate the Python venv
source ./bin/workon.sh

# Run the debug build
inv dev.cmake --build=Debug --clean
inv dev.cc faabric_dist_tests
inv dev.cc faabric_dist_test_server
inv dev.cc planner_server

popd >> /dev/null
