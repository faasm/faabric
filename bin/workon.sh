#!/bin/bash

# ----------------------------
# Container-specific settings
# ----------------------------

MODE="undetected"
if [[ -z "$FAABRIC_DOCKER" ]]; then

    THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"
    PROJ_ROOT="${THIS_DIR}/.."
    VENV_PATH="${PROJ_ROOT}/venv-bm"
    export FAABRIC_BUILD_DIR="${PROJ_ROOT}/build"

    # Normal terminal
    MODE="terminal"
else
    # Running inside the container, we know the project root
    PROJ_ROOT="/code/faabric"
    VENV_PATH="${PROJ_ROOT}/venv"
    export FAABRIC_BUILD_DIR="/build/faabric"

    # Use containerised redis
    alias redis-cli="redis-cli -h redis"

    MODE="container"
fi

pushd ${PROJ_ROOT}>>/dev/null

# ----------------------------
# Virtualenv
# ----------------------------

if [ ! -d ${VENV_PATH} ]; then
    ${PROJ_ROOT}/bin/create_venv.sh
fi

export VIRTUAL_ENV_DISABLE_PROMPT=1
source ${VENV_PATH}/bin/activate

# ----------------------------
# Invoke tab-completion
# (http://docs.pyinvoke.org/en/stable/invoke.html#shell-tab-completion)
# ----------------------------

_complete_invoke() {
    local candidates
    candidates=`invoke --complete -- ${COMP_WORDS[*]}`
    COMPREPLY=( $(compgen -W "${candidates}" -- $2) )
}

# If running from zsh, run autoload for tab completion
if [ "$(ps -o comm= -p $$)" = "zsh" ]; then
    autoload bashcompinit
    bashcompinit
fi
complete -F _complete_invoke -o default invoke inv

# ----------------------------
# Environment vars
# ----------------------------

VERSION_FILE=${PROJ_ROOT}/VERSION
export FAABRIC_ROOT=$(pwd)
export FAABRIC_VERSION=$(cat ${VERSION_FILE})

export PS1="(faabric) $PS1"

export PATH=${FAABRIC_BUILD_DIR}/static/bin:${PATH}

# -----------------------------
# Splash
# -----------------------------

echo ""
echo "----------------------------------"
echo "Faabric CLI"
echo "Version: ${FAABRIC_VERSION}"
echo "Project root: $(pwd)"
echo "Build dir: ${FAABRIC_BUILD_DIR}"
echo "Mode: ${MODE}"
echo "----------------------------------"
echo ""

popd >> /dev/null
