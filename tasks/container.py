from os.path import join, exists
from subprocess import run

from invoke import task, Failure

from tasks.util.env import get_version, PROJ_ROOT


IMAGES = [
    "base",
]


@task
def build(ctx, nocache=False, push=False):
    """
    Build current version of container images
    """
    this_version = get_version()

    for container in IMAGES:
        tag_name = "faabric/{}:{}".format(container, this_version)

        if nocache:
            no_cache_str = "--no-cache"
        else:
            no_cache_str = ""

        dockerfile = join(
            PROJ_ROOT, "docker", "{}.dockerfile".format(container)
        )
        if not exists(dockerfile):
            raise Failure("Invalid container: {}".format(container))

        build_cmd = [
            "docker build",
            no_cache_str,
            "-t {}".format(tag_name),
            "--build-arg FAABRIC_VERSION={}".format(this_version),
            "-f {}".format(dockerfile),
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

    for container in IMAGES:
        cmd = "docker push faabric/{}:{}".format(container, this_version)
        
        print(cmd)
        run(
            cmd,
            shell=True,
            check=True,
        )
