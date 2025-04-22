from invoke import task
from os.path import join
from subprocess import run
from tasks.util.env import CR_NAME, LLVM_VERSION, PROJ_ROOT, get_version

FAABRIC_IMAGE_NAME = "faabric"
FAABRIC_BASE_IMAGE_NAME = "faabric-base"
FAABRIC_PLANNER_IMAGE_NAME = "planner"


def _get_docker_tag(img_name):
    ver = get_version()
    return "{}/{}:{}".format(CR_NAME, img_name, ver)


def _do_container_build(name, nocache=False, push=False, build_args={}):
    tag_name = _get_docker_tag(name)

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
    ]

    for key, value in build_args.items():
        build_cmd.append("--build-arg {}={}".format(key, value))

    build_cmd.append(".")
    build_cmd = " ".join(build_cmd)
    run(build_cmd, shell=True, check=True, env={"DOCKER_BUILDKIT": "1"})

    if push:
        _do_push(name)


def _do_push(name):
    tag_name = _get_docker_tag(name)

    cmd = "docker push {}".format(tag_name)
    print(cmd)
    run(cmd, shell=True, check=True)


@task(iterable=["c"])
def build(ctx, c, nocache=False, push=False):
    """
    Build containers for faabric. Targets are: `faabric`, and `faabric-base`
    """
    build_args = {"FAABRIC_VERSION": get_version()}
    for ctr in c:
        if ctr == "faabric":
            img_name = FAABRIC_IMAGE_NAME
        elif ctr == "faabric-base":
            build_args["LLVM_VERSION_MAJOR"] = LLVM_VERSION.split(".")[0]
            img_name = FAABRIC_BASE_IMAGE_NAME
        elif ctr == "planner":
            img_name = FAABRIC_PLANNER_IMAGE_NAME
        else:
            print("Unrecognised container name: {}".format(ctr))
            raise RuntimeError("Unrecognised container name")

        _do_container_build(
            img_name, nocache=nocache, push=push, build_args=build_args
        )


@task(iterable=["c"])
def push(ctx, c):
    """
    Push containers for faabric. Targets are: `faabric`, and `faabric-base`
    """
    for ctr in c:
        if ctr == "faabric":
            img_name = FAABRIC_IMAGE_NAME
        elif ctr == "faabric-base":
            img_name = FAABRIC_BASE_IMAGE_NAME
        elif ctr == "planner":
            img_name = FAABRIC_PLANNER_IMAGE_NAME
        else:
            print("Unrecognised container name: {}".format(ctr))
            raise RuntimeError("Unrecognised container name")

        _do_push(img_name)
