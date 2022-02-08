from os import makedirs
from shutil import rmtree
from os.path import exists, join
from subprocess import run

from tasks.util.env import (
    PROJ_ROOT,
    FAABRIC_SHARED_BUILD_DIR,
    FAABRIC_STATIC_BUILD_DIR,
    FAABRIC_INSTALL_PREFIX,
)

from invoke import task

DOCS_DIR = join(PROJ_ROOT, "docs")
DOXY_OUT_DIR = join(DOCS_DIR, "doxygen", "xml")
SPHINX_OUT_DIR = join(DOCS_DIR, "sphinx")


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
