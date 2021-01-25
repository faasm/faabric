#!/bin/bash

set -e

THIS_DIR=$(dirname $(readlink -f $0))
PROJ_ROOT=${THIS_DIR}/..

pushd ${PROJ_ROOT} >> /dev/null

source ./mpi-native/mpi-native.env

for w in $(ls ./mpi-native/examples/*.cpp);
do
    example=$(basename $w ".cpp")
    if [[ $example == mpi_* ]]; then
        sed -i '$ d' ${ENV_FILE}
        echo "MPI_EXAMPLE=${example}" >> ${ENV_FILE}
        echo "----------------------------------------"
        echo "Running MPI native test:"
        echo "    - MPI_EXAMPLE ${example}"
        echo "    - MPI_WORLD_SIZE ${MPI_WORLD_SIZE}"
        echo "----------------------------------------"
        ./mpi-native/run.sh || exit 1
        echo "----------------------------------------"
        echo "             SUCCESS!"
        echo "----------------------------------------"
    fi
done

popd >> /dev/null

echo "----------------------------------------"
echo "         All examples ran OK!"
echo "----------------------------------------"

exit 0

