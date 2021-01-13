from os import makedirs, environ
from shutil import rmtree
from os.path import join, exists
from copy import copy
from subprocess import run, Popen

from tasks.util.env import PROJ_ROOT, FAABRIC_INSTALL_PREFIX

from invoke import task

import requests

EXAMPLES_DIR = join(PROJ_ROOT, "examples")
BUILD_DIR = join(EXAMPLES_DIR, "build")

INCLUDE_DIR = "{}/include".format(FAABRIC_INSTALL_PREFIX)
LIB_DIR = "{}/lib".format(FAABRIC_INSTALL_PREFIX)

FAABRIC_SERVICE_NAME = "cli"
MPI_DEFAULT_WORLD_SIZE = 1


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

    # Start up np - 1 faabric_processes. Note that if there are any other
    # scaled out processes they will be stopped and removed. Additionally,
    # running /bin/cli.sh whilst having a scaled out client will stop & remove
    # them as well.
    scale_up_cmd = "docker-compose up -d --scale {}={} --no-recreate".format(
        FAABRIC_SERVICE_NAME, np
    )
    run(scale_up_cmd, shell=True, check=True)

    run(exe_path, env=shell_env, shell=True, check=True)


@task
def invoke_mpi(ctx, host="0.0.0.0", port="8080"):
    """
    Invoke MPI function through HTTP handler
    """
    # The host:port address must match that of the HTTP Endpoint
    url = "http://{}:{}".format(host, port)
    msg = {"user": "mpi", "function": "faabric", "mpi_world_size": 1}
    response = requests.post(url, json=msg, headers=None)
    print(response.text)


@task
def terminate_mpi(ctx):
    """
    Terminate an MPI execution
    """
    # This will stop and remove all containers scaled out (i.e. invoked using
    # the --scale flag) and leave those specified in the docker-compose file.
    scale_down_cmd = "docker-compose up -d --no-recreate"
    run(scale_down_cmd, shell=True, check=True)
