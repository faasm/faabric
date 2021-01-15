from copy import copy
from os import environ
from os.path import exists, join
from subprocess import run

from tasks.util.env import PROJ_ROOT, FAABRIC_INSTALL_PREFIX

from invoke import task

EXAMPLES_DIR = join(PROJ_ROOT, "examples")
BUILD_DIR = join(EXAMPLES_DIR, "build")

INCLUDE_DIR = "{}/include".format(FAABRIC_INSTALL_PREFIX)
LIB_DIR = "{}/lib".format(FAABRIC_INSTALL_PREFIX)

# As it appears in the docker-compose file
FAABRIC_WORKER_NAME = "cli"
MPI_DEFAULT_WORLD_SIZE = 1


def _find_mpi_hosts():
    """
    Return all deployed MPI hosts. This is, active containers based on the
    client image. This won't work if we change the container name used
    to deploying replicas.
    """
    docker_cmd = "docker ps --format '{{.Names}}'"

    active_containers = run(
        docker_cmd, shell=True, check=True, capture_output=True
    )

    # Format output and filter only 'client' containers
    mpi_hosts = [
        c
        for c in active_containers.stdout.decode("utf-8").split("\n")
        if FAABRIC_WORKER_NAME in c
    ]

    return mpi_hosts


# TODO: eventually move all MPI examples to ./examples/mpi
@task
def execute(ctx, example, clean=False, np=MPI_DEFAULT_WORLD_SIZE):
    """
    Runs an MPI example
    """
    exe_path = join("./examples/build/", example)

    if not exists(exe_path):
        raise RuntimeError("Did not find {} as expected".format(exe_path))

    shell_env = copy(environ)
    shell_env.update(
        {
            "LD_LIBRARY_PATH": LIB_DIR,
        }
    )

    # Start up `np` faabric instances. Note that if there are any other
    # scaled out processes they will be stopped and removed. Additionally,
    # running /bin/cli.sh whilst having a scaled out cluster will stop & remove
    # them as well.
    scale_up_cmd = "docker-compose up -d --scale {}={} {}".format(
        FAABRIC_WORKER_NAME,
        np,
        "--force-recreate" if clean else "--no-recreate",
    )
    print(scale_up_cmd)
    run(scale_up_cmd, shell=True, check=True)

    # Get a list of all hosts
    mpi_hosts = _find_mpi_hosts()

    # Run the binary in each host
    exe_cmd_fmt = (
        "docker exec -{}t {} bash -c 'export LD_LIBRARY_PATH={}; {} {} {}'"
    )
    # Start first all non-root instances in detached mode (-dt)
    for host in mpi_hosts[1:]:
        exe_cmd = exe_cmd_fmt.format("d", host, LIB_DIR, exe_path, "", "")
        print(exe_cmd)
        run(exe_cmd, shell=True)
    # Start the root process (this will bootstrap the execution)
    exe_cmd = (
        exe_cmd_fmt.format("", mpi_hosts[0], LIB_DIR, exe_path, "root", np),
    )
    print(exe_cmd)
    run(exe_cmd, shell=True)


@task
def clean(ctx, force=False):
    """
    Clean environment from failed deployments
    """
    docker_cmd = "docker-compose up -d {}".format(
        "--force-recreate" if force else "--no-recreate"
    )
    print(docker_cmd)
    run(docker_cmd, shell=True, check=True)
