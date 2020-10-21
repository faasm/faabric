from invoke import task

from tasks.util.env import get_version, PROJ_ROOT

from subprocess import run


@task
def tag(ctx, force=False):
    """
    Creates git tag from the current tree
    """
    git_tag = "v{}".format(get_version())
    run(
        "git tag {} {}".format("--force" if force else "", git_tag),
        shell=True,
        check=True,
        cwd=PROJ_ROOT,
    )

    run(
        "git push origin {} {}".format("--force" if force else "", git_tag),
        shell=True,
        check=True,
        cwd=PROJ_ROOT,
    )
