#!/bin/bash
# Scripted some of the material here:
# https://github.com/brendangregg/FlameGraph
# NOTE - This script must be run as root.

set -e

FLAME_GRAPH_DIR="/home/csegarra/sof/FlameGraph"
BINARY="/build/faasm/bin/lammps_runner"
LAMMPS_BINARY="/code/experiment-lammps/third-party/lammps/install-native/bin/lmp"
FILE_PREFIX="prof_lammps"
TIME=60

pushd ${FLAME_GRAPH_DIR} >> /dev/null

# Get the PID to monitor
LAMMPS_PID=$(ps -ef | awk '$8=="/build/faasm/bin/lammps_runner" {print $2}')
# LAMMPS_PID=$(ps -ef | awk '$8=="/code/experiment-lammps/third-party/lammps/install-native/bin/lmp" {print $2}')
echo "LAMMPS Runner PID: ${LAMMPS_PID}"

# Record events
echo "Starting perf recording"

# Run either against all processes (-a) or one pid (-p)
sudo perf record -F 99 -p ${LAMMPS_PID} -g -- sleep ${TIME}
# sudo perf record -F 99 -a -g -- sleep ${TIME}
echo "Done recording!"
perf script -f > ${FILE_PREFIX}.perf

# Collapse stacks
# TODO change color palette
echo "Collapsing stacks"
./stackcollapse-perf.pl --all ${FILE_PREFIX}.perf > ${FILE_PREFIX}.folded
echo "-- Generating SVG"
./flamegraph.pl ${FILE_PREFIX}.folded > ${FILE_PREFIX}.svg
echo "-- SVG generated at: ${FLAME_GRAPH_DIR}/${FILE_PREFIX}.svg"
echo "-- Open it with a browser."

popd >> /dev/null

