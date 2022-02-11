from os import makedirs
from os.path import join
from subprocess import run

from tasks.util.env import PROJ_ROOT

from invoke import task

DOCS_DIR = join(PROJ_ROOT, "docs")
SPHINX_OUT_DIR = join(DOCS_DIR, "sphinx")


@task(default=True)
def generate(ctx):
    makedirs(SPHINX_OUT_DIR, exist_ok=True)

    run(
        "sphinx-build -b html {} {}".format(DOCS_DIR, SPHINX_OUT_DIR),
        cwd=DOCS_DIR,
        check=True,
        shell=True,
    )
