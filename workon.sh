#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
pushd ${THIS_DIR} >> /dev/null

# ----------------------------
# Virtualenv
# ----------------------------

if [ ! -d "venv" ]; then
    python3 -m venv venv --system-site-packages
fi

export VIRTUAL_ENV_DISABLE_PROMPT=1
source venv/bin/activate

# ----------------------------
# Invoke tab-completion (http://docs.pyinvoke.org/en/stable/invoke.html#shell-tab-completion)
# ----------------------------

_complete_invoke() {
    local candidates
    candidates=`invoke --complete -- ${COMP_WORDS[*]}`
    COMPREPLY=( $(compgen -W "${candidates}" -- $2) )
}

complete -F _complete_invoke -o default invoke inv

# -----------------------------
# Terminal
# -----------------------------

export PS1="(faabric) $PS1"

popd >> /dev/null
