from subprocess import run

from invoke import task

from tasks.util.env import get_docker_tag


@task
def build(ctx, nocache=False, push=False):
    """
    Build current version of container
    """
    tag_name = get_docker_tag()

    if nocache:
        no_cache_str = "--no-cache"
    else:
        no_cache_str = ""

    build_cmd = [
        "docker build",
        no_cache_str,
        "-t {}".format(tag_name),
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
    tag_name = get_docker_tag()

    cmd = "docker push {}".format(tag_name)

    print(cmd)
    run(cmd, shell=True, check=True)
