from os.path import join, exists

from invoke import task, Failure

from tasks.util.env import get_version, PROJ_ROOT


@task(iterable=["c"])
def build(ctx, c, nocache=False, push=False):
    """
    Build container images
    """
    this_version = get_version()

    for container in c:
        tag_name = "faabric/{}:{}".format(container, this_version)

        if nocache:
            no_cache_str = "--no-cache"
        else:
            no_cache_str = ""

        dockerfile = join(PROJ_ROOT, "docker", "{}.dockerfile".format(container))
        if not exists(dockerfile):
            raise Failure("Invalid container: {}".format(container))

        cmd = "docker build {} -t {} --build-arg FAABRIC_VERSION={} -f {} .".format(no_cache_str, tag_name,
                                                                                    this_version,
                                                                                    dockerfile)
        print(cmd)
        ctx.run(cmd, env={
            "DOCKER_BUILDKIT": "1"
        })

        if push:
            ctx.run("docker push faabric/{}:{}".format(container, this_version))
