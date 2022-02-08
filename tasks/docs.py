from os import makedirs, environ
from os.path import join
from subprocess import run

from tasks.util.env import PROJ_ROOT

from invoke import task

DOCS_DIR = join(PROJ_ROOT, "docs")
DOXY_OUT_DIR = join(DOCS_DIR, "doxygen", "xml")
SPHINX_OUT_DIR = join(DOCS_DIR, "sphinx")

IS_READ_THE_DOCS = environ.get("READTHEDOCS", None) == "True"


@task(default=True)
def doxy(ctx):
    run("doxygen", cwd=DOCS_DIR, check=True, shell=True)


@task
def sphinx(ctx):
    makedirs(SPHINX_OUT_DIR, exist_ok=True)

    run(
        "sphinx-build -b html {} {}".format(DOCS_DIR, SPHINX_OUT_DIR),
        cwd=DOCS_DIR,
        check=True,
        shell=True,
    )
