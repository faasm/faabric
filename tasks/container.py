from subprocess import run

from invoke import task

from tasks.util.env import get_version


@task
def build(ctx, nocache=False, push=False):
    """
    Build current version of container
    """
    this_version = get_version()

    tag_name = "faabric/base:{}".format(this_version)

    if nocache:
        no_cache_str = "--no-cache"
    else:
        no_cache_str = ""

    build_cmd = [
        "docker build",
        no_cache_str,
        "-t {}".format(tag_name),
        "--build-arg FAABRIC_VERSION={}".format(this_version),
        ".",
    ]
    build_cmd = " ".join(build_cmd)

    print(build_cmd)
    run(build_cmd, shell=True, check=True, env={"DOCKER_BUILDKIT": "1"})

    if push:
        push(ctx)


@task
def push(ctx):
    """
    Push current version of container images
    """
    this_version = get_version()

    cmd = "docker push faabric/base:{}".format(this_version)

    print(cmd)
    run(
        cmd,
        shell=True,
        check=True,
    )
