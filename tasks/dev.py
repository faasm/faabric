from os import makedirs
from shutil import rmtree
from os.path import exists
from subprocess import run

from tasks.util.env import (
    PROJ_ROOT,
    FAABRIC_SHARED_BUILD_DIR,
    FAABRIC_STATIC_BUILD_DIR,
    FAABRIC_INSTALL_PREFIX,
)

from invoke import task


@task
def cmake(ctx, clean=False, shared=False, build="Debug", sanitise_mode="None"):
    """
    Configures the build
    """
    build_dir = (
        FAABRIC_SHARED_BUILD_DIR if shared else FAABRIC_STATIC_BUILD_DIR
    )

    if clean and exists(build_dir):
        rmtree(build_dir)

    if not exists(build_dir):
        makedirs(build_dir)

    build_types = ["Release", "Debug"]
    if build not in build_types:
        raise RuntimeError("Expected build to be in {}".format(build_types))

    cmd = [
        "cmake",
        "-GNinja",
        "-DCMAKE_INSTALL_PREFIX={}".format(FAABRIC_INSTALL_PREFIX),
        "-DCMAKE_BUILD_TYPE={}".format(build),
        "-DBUILD_SHARED_LIBS={}".format("ON" if shared else "OFF"),
        "-DCMAKE_CXX_COMPILER=/usr/bin/clang++-13",
        "-DCMAKE_C_COMPILER=/usr/bin/clang-13",
        "-DFAABRIC_USE_SANITISER={}".format(sanitise_mode),
        PROJ_ROOT,
    ]

    run(" ".join(cmd), check=True, shell=True, cwd=build_dir)


@task
def cc(ctx, target, shared=False):
    """
    Compile the given target
    """
    build_dir = (
        FAABRIC_SHARED_BUILD_DIR if shared else FAABRIC_STATIC_BUILD_DIR
    )

    run(
        "cmake --build . --target {}".format(target),
        check=True,
        cwd=build_dir,
        shell=True,
    )


@task
def install(ctx, target, shared=False):
    """
    Install the given target
    """
    build_dir = (
        FAABRIC_SHARED_BUILD_DIR if shared else FAABRIC_STATIC_BUILD_DIR
    )

    run(
        "ninja install {}".format(target),
        check=True,
        cwd=build_dir,
        shell=True,
    )


@task
def sanitise(ctx, mode, target="faabric_tests", noclean=False, shared=False):
    """
    Build the tests with different sanitisers
    """

    cmake(
        ctx,
        clean=(not noclean),
        shared=shared,
        build="Debug",
        sanitise_mode=mode,
    )

    cc(ctx, target, shared=shared)
