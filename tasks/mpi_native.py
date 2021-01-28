from shutil import rmtree
from os.path import join, exists
from subprocess import run

from tasks.util.env import PROJ_ROOT, FAABRIC_INSTALL_PREFIX

from invoke import task

MPI_EXAMPLES_DIR = join(PROJ_ROOT, "mpi-native/examples")
MPI_BUILD_DIR = join(MPI_EXAMPLES_DIR, "build")

INCLUDE_DIR = "{}/include".format(FAABRIC_INSTALL_PREFIX)
LIB_DIR = "{}/lib".format(FAABRIC_INSTALL_PREFIX)


@task()
def build_mpi(ctx, clean=False):
    """
    Builds the examples
    """

    if clean and exists(MPI_BUILD_DIR):
        rmtree(MPI_BUILD_DIR)

    if not exists(MPI_BUILD_DIR):
        makedirs(MPI_BUILD_DIR)

    # Cmake
    cmake_cmd = " ".join(
        [
            "cmake",
            "-GNinja",
            "-DCMAKE_BUILD_TYPE=Debug",
            "-DCMAKE_CXX_FLAGS=-I{}".format(INCLUDE_DIR),
            "-DCMAKE_EXE_LINKER_FLAGS=-L{}".format(LIB_DIR),
            "-DCMAKE_CXX_COMPILER=/usr/bin/clang++-10",
            "-DCMAKE_C_COMPILER=/usr/bin/clang-10",
            MPI_EXAMPLES_DIR,
        ]
    )
    print(cmake_cmd)

    run(
        cmake_cmd,
        cwd=MPI_BUILD_DIR,
        shell=True,
        check=True,
    )

    # Build
    run(
        "cmake --build . --target all_examples",
        cwd=MPI_BUILD_DIR,
        shell=True,
        check=True,
    )
