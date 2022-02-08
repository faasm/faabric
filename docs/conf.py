from os.path import dirname, realpath, join
from subprocess import run
from os import environ

DOCS_ROOT = dirname(realpath(__file__))
DOXYGEN_OUT = join(DOCS_ROOT, "doxygen", "xml")
APIDOC_OUT_DIR = join(DOCS_ROOT, "apidoc")

# Generate doxygen files in RTD
IS_READ_THE_DOCS = environ.get("READTHEDOCS", None) == "True"
if IS_READ_THE_DOCS:
    run("doxygen", cwd=DOCS_ROOT, check=True, shell=True)

    run(
        "breathe-apidoc {} -o {}".format(DOXYGEN_OUT, APIDOC_OUT_DIR),
        cwd=DOCS_ROOT,
        check=True,
        shell=True,
    )

project = "Faabric"
copyright = "2022, Simon Shillaker"
author = "Simon Shillaker"

extensions = ["breathe", "myst_parser"]

templates_path = ["source/templates"]
html_static_path = ["source/static"]

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "alabaster"

breathe_projects = {"Faabric": DOXYGEN_OUT}
breathe_default_project = "Faabric"
