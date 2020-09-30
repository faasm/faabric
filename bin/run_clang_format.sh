#!/bin/bash

set -e

if [ -z "$1" ]; then
    echo "Must specify a directory (use . for this dir)"
    exit 1
fi

TARGET_DIR=$1
echo "Running clang-format on ${TARGET_DIR}"

pushd ${TARGET_DIR} >> /dev/null

# Find all source files using Git to automatically respect .gitignore
FILES=$(git ls-files "*.h" "*.cpp" "*.c") 
for f in $FILES
do
    echo "Format $f"
    clang-format-10 -i $f
done

popd >> /dev/null

