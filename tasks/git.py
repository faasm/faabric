from invoke import task

from tasks.util.env import get_version, PROJ_ROOT

from subprocess import run


@task
def tag(ctx):
    """
    Creates git tag from the current tree
    """
    git_tag = "v{}".format(get_version())
    run("git tag {}".format(git_tag), shell=True, check=True, cwd=PROJ_ROOT)

    run(
        "git push origin {}".format(git_tag),
        shell=True,
        check=True,
        cwd=PROJ_ROOT,
    )
