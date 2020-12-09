from os import makedirs
from shutil import rmtree
from os.path import join, exists
from subprocess import run

from tasks.util.env import PROJ_ROOT, FAABRIC_INSTALL_PREFIX

from invoke import task

EXAMPLES_DIR = join(PROJ_ROOT, "examples")
BUILD_DIR = join(EXAMPLES_DIR, "build")


@task(default=True)
def build(ctx, clean=False, shared=False):
    """
    Builds the examples
    """
    if clean and exists(BUILD_DIR):
        rmtree(BUILD_DIR)

    if not exists(BUILD_DIR):
        makedirs(BUILD_DIR)

    include_dir = "{}/include".format(FAABRIC_INSTALL_PREFIX)

    # Cmake
    run(
        " ".join(
            [
                "cmake",
                "-GNinja",
                "-DCMAKE_BUILD_TYPE=Release",
                "-DCMAKE_CXX_FLAGS=-I{}".format(include_dir),
                "-DBUILD_SHARED_LIBS={}".format("ON" if shared else "OFF"),
                "-DCMAKE_CXX_COMPILER=/usr/bin/clang++-10",
                "-DCMAKE_C_COMPILER=/usr/bin/clang-10",
                EXAMPLES_DIR,
            ]
        ),
        shell=True,
        cwd=BUILD_DIR,
    )

    # Build
    run(
        "cmake --build . --target all_examples",
        cwd=BUILD_DIR,
        shell=True,
    )
