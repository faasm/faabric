from os import makedirs
from os.path import exists, join
from shutil import rmtree
from subprocess import run
from tasks.util.env import (
    FAABRIC_SHARED_BUILD_DIR,
    FAABRIC_STATIC_BUILD_DIR,
    FAABRIC_INSTALL_PREFIX,
    LLVM_VERSION_MAJOR,
    PROJ_ROOT,
)

from invoke import task


@task
def cmake(
    ctx,
    clean=False,
    shared=False,
    build="Debug",
    sanitiser="None",
    coverage=False,
    prof=False,
    cpu=None,
):
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
        "-DCMAKE_CXX_COMPILER=/usr/bin/clang++-{}".format(LLVM_VERSION_MAJOR),
        "-DCMAKE_C_COMPILER=/usr/bin/clang-{}".format(LLVM_VERSION_MAJOR),
        "-DFAABRIC_USE_SANITISER={}".format(sanitiser),
        "-DFAABRIC_SELF_TRACING=ON" if prof else "",
        "-DFAABRIC_CODE_COVERAGE=ON" if coverage else "",
        "-DFAABRIC_TARGET_CPU={}".format(cpu) if cpu else "",
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
        sanitiser=mode,
    )

    cc(ctx, target, shared=shared)


@task
def coverage_report(ctx, file_in, file_out):
    """
    Generate code coverage report
    """
    tmp_file = "tmp_gha.profdata"

    # First, merge in the raw profiling data
    llvm_cmd = [
        "llvm-profdata-{}".format(LLVM_VERSION_MAJOR),
        "merge -sparse {}".format(file_in),
        "-o {}".format(tmp_file),
    ]
    llvm_cmd = " ".join(llvm_cmd)
    run(llvm_cmd, shell=True, check=True, cwd=PROJ_ROOT)

    # Second, generate the coverage report
    llvm_cmd = [
        "llvm-cov-{} show".format(LLVM_VERSION_MAJOR),
        "--ignore-filename-regex=/code/faabric/tests/*",
        join(FAABRIC_STATIC_BUILD_DIR, "bin", "faabric_tests"),
        "-instr-profile={}".format(tmp_file),
        "> {}".format(file_out),
    ]
    llvm_cmd = " ".join(llvm_cmd)
    run(llvm_cmd, shell=True, check=True, cwd=PROJ_ROOT)
