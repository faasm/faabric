from os.path import dirname, realpath, join

DOCS_ROOT = dirname(realpath(__file__))
DOXYGEN_OUT = join(DOCS_ROOT, "doxygen", "xml")

project = "Faabric"
copyright = "2022, Simon Shillaker"
author = "Simon Shillaker"

extensions = ["breathe"]

templates_path = ["templates"]

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "alabaster"

html_static_path = ["static"]

breathe_projects = {"Faabric": DOXYGEN_OUT}
breathe_default_project = "Faabric"
