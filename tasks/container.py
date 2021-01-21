from subprocess import run
from os.path import join

from invoke import task
from tasks.util.env import get_version, PROJ_ROOT

FAABRIC_IMAGE_NAME = "faabric"
GRPC_IMAGE_NAME = "grpc-root"
MPI_NATIVE_IMAGE_NAME = "faabric-mpi-native"


def _get_docker_tag(img_name):
    ver = get_version()
    return "faasm/{}:{}".format(img_name, ver)


def _do_container_build(name, nocache=False, push=False):
    tag_name = _get_docker_tag(name)
    ver = get_version()

    if nocache:
        no_cache_str = "--no-cache"
    else:
        no_cache_str = ""

    dockerfile = join(PROJ_ROOT, "docker", "{}.dockerfile".format(name))

    build_cmd = [
        "docker build",
        no_cache_str,
        "-t {}".format(tag_name),
        "-f {}".format(dockerfile),
        "--build-arg FAABRIC_VERSION={}".format(ver),
        ".",
    ]
    build_cmd = " ".join(build_cmd)

    print(build_cmd)
    run(build_cmd, shell=True, check=True, env={"DOCKER_BUILDKIT": "1"})

    if push:
        _do_push(name)


def _do_push(name):
    tag_name = _get_docker_tag(name)

    cmd = "docker push {}".format(tag_name)
    print(cmd)
    run(cmd, shell=True, check=True)


@task
def build(ctx, nocache=False, push=False):
    """
    Build current version of faabric container
    """
    _do_container_build(FAABRIC_IMAGE_NAME, nocache=nocache, push=push)


@task
def build_grpc(ctx, nocache=False, push=False):
    """
    Build current base gRPC container
    """
    _do_container_build(GRPC_IMAGE_NAME, nocache=nocache, push=push)


@task
def build_mpi_native(ctx, nocache=False, push=False):
    """
    Build current native MPI container
    """
    _do_container_build(MPI_NATIVE_IMAGE_NAME, nocache=nocache, push=push)


@task
def push(ctx):
    """
    Push current version of faabric container
    """
    _do_push(FAABRIC_IMAGE_NAME)


@task
def push_grpc(ctx):
    """
    Push current version of gRPC container
    """
    _do_push(GRPC_IMAGE_NAME)


@task
def push_mpi_native(ctx):
    """
    Push current version of the native MPI container
    """
    _do_push(MPI_NATIVE_IMAGE_NAME)
