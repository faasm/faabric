from copy import copy
from invoke import task
from os import makedirs, environ
from os.path import join, exists
from shutil import rmtree
from subprocess import run
from tasks.util.env import (
    FAABRIC_CONAN_CACHE,
    FAABRIC_INSTALL_PREFIX,
    LLVM_VERSION_MAJOR,
    PROJ_ROOT,
)


EXAMPLES_DIR = join(PROJ_ROOT, "examples")
BUILD_DIR = join(EXAMPLES_DIR, "build")
INCLUDE_DIR = "{}/include".format(FAABRIC_INSTALL_PREFIX)
LIB_DIR = "{}/lib".format(FAABRIC_INSTALL_PREFIX)


@task(default=True)
def build(ctx, clean=False):
    """
    Builds the examples
    """
    if clean and exists(BUILD_DIR):
        rmtree(BUILD_DIR)

    if not exists(BUILD_DIR):
        makedirs(BUILD_DIR)

    conan_cache = f"{FAABRIC_CONAN_CACHE}/release"
    if not exists(conan_cache):
        print(f"ERROR: expected conan cache in {conan_cache}")
        print("ERROR: make sure to run 'inv dev.conan' first")
        raise RuntimeError(f"Expected conan cache in {conan_cache}")

    # Cmake
    cmake_cmd = " ".join(
        [
            "cmake",
            "-GNinja",
            "-DCMAKE_BUILD_TYPE=Release",
            f"-DCMAKE_TOOLCHAIN_FILE={conan_cache}/conan_toolchain.cmake",
            "-DCMAKE_CXX_FLAGS=-I{}".format(INCLUDE_DIR),
            "-DCMAKE_EXE_LINKER_FLAGS=-L{}".format(LIB_DIR),
            "-DCMAKE_CXX_COMPILER=/usr/bin/clang++-{}".format(
                LLVM_VERSION_MAJOR
            ),
            "-DCMAKE_C_COMPILER=/usr/bin/clang-{}".format(LLVM_VERSION_MAJOR),
            EXAMPLES_DIR,
        ]
    )
    print(cmake_cmd)

    run(
        cmake_cmd,
        cwd=BUILD_DIR,
        shell=True,
        check=True,
    )

    # Build
    run(
        "cmake --build . --target all_examples",
        cwd=BUILD_DIR,
        shell=True,
        check=True,
    )


@task
def execute(ctx, example):
    """
    Runs the given example
    """
    exe_path = join(BUILD_DIR, example)

    if not exists(exe_path):
        raise RuntimeError("Did not find {} as expected".format(exe_path))

    shell_env = copy(environ)
    shell_env.update(
        {
            "LD_LIBRARY_PATH": LIB_DIR,
        }
    )

    run(exe_path, env=shell_env, shell=True, check=True)
