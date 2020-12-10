#!/bin/bash

set -e

BUILD_PATH=/build/faabric/static/
CONFIG=${FAABRIC_ROOT}/.clang-tidy

FILES=$(git ls-files {src,tests}/{"*.h","*.cpp","*.c"})

if [ -z "$1" ]; then
    :
else
    pushd $1 >> /dev/null
    FILES=$(git ls-files {"*.h","*.cpp","*.c"})
fi

# Run clang-tidy on a set of files
run-clang-tidy-10.py \
    -config '' \
    -j `nproc` \
    -fix \
    -format \
    -style 'file' \
    -p ${BUILD_PATH} \
    -quiet \
    ${FILES} > /dev/null

popd >> /dev/null || true

