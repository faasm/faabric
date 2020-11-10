#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROJ_ROOT=${THIS_DIR}/..
pushd ${PROJ_ROOT}>>/dev/null

# ----------------------------
# Virtualenv
# ----------------------------

if [ ! -d "venv" ]; then
    python3 -m venv venv 
fi

export VIRTUAL_ENV_DISABLE_PROMPT=1
source venv/bin/activate

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

export PATH=/build/faabric/bin:${PATH}

# -----------------------------
# Splash
# -----------------------------

echo ""
echo "----------------------------------"
echo "Faabric CLI"
echo "Version: ${FAABRIC_VERSION}"
echo "Project root: $(pwd)"
echo "----------------------------------"
echo ""

popd >> /dev/null
