#!/bin/bash

set -e

THIS_DIR=$(dirname $(readlink -f $0))
PROJ_ROOT=${THIS_DIR}/..

# Set different virtual environment paths so that these don't clash when
# mounting the code in a development container
VENV_PATH="undetected"
if [[ -z "$FAABRIC_DOCKER" ]]; then
    VENV_PATH="${PROJ_ROOT}/venv-bm"
else
    VENV_PATH="${PROJ_ROOT}/venv"
fi

PIP=${VENV_PATH}/bin/pip3

function pip_cmd {
    source ${VENV_PATH}/bin/activate && ${PIP} "$@"
}

pushd ${PROJ_ROOT} >> /dev/null

if [ ! -d ${VENV_PATH} ]; then
    python3 -m venv ${VENV_PATH}
fi

pip_cmd install -U pip
pip_cmd install -U setuptools wheel
pip_cmd install -r requirements.txt

touch ${VENV_PATH}/faabric_venv.BUILT

popd >> /dev/null
