from os import makedirs, environ
from shutil import rmtree
from os.path import join, exists
from copy import copy
from subprocess import run, Popen

from tasks.util.env import PROJ_ROOT, FAABRIC_INSTALL_PREFIX

from invoke import task

EXAMPLES_DIR = join(PROJ_ROOT, "examples")
BUILD_DIR = join(EXAMPLES_DIR, "build")

INCLUDE_DIR = "{}/include".format(FAABRIC_INSTALL_PREFIX)
LIB_DIR = "{}/lib".format(FAABRIC_INSTALL_PREFIX)

MPI_DEFAULT_WORLD_SIZE = 5


@task(default=True)
def build(ctx, clean=False):
    """
    Builds the examples
    """
    if clean and exists(BUILD_DIR):
        rmtree(BUILD_DIR)

    if not exists(BUILD_DIR):
        makedirs(BUILD_DIR)

    # Cmake
    cmake_cmd = " ".join(
        [
            "cmake",
            "-GNinja",
            "-DCMAKE_BUILD_TYPE=Release",
            "-DCMAKE_CXX_FLAGS=-I{}".format(INCLUDE_DIR),
            "-DCMAKE_EXE_LINKER_FLAGS=-L{}".format(LIB_DIR),
            "-DCMAKE_CXX_COMPILER=/usr/bin/clang++-10",
            "-DCMAKE_C_COMPILER=/usr/bin/clang-10",
            EXAMPLES_DIR,
        ]
    )
    print(cmake_cmd)

    run(
        cmake_cmd,
        shell=True,
        cwd=BUILD_DIR,
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


@task
def execute_mpi(ctx, example, np=MPI_DEFAULT_WORLD_SIZE):
    """
    Runs an MPI example
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

    procs = [
        Popen(exe_path, env=shell_env, shell=True) for _ in range(np)
    ]
    for p in procs:
        p.wait()
