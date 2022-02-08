from os import environ
from os.path import dirname, realpath, join, expanduser

HOME_DIR = expanduser("~")
PROJ_ROOT = dirname(dirname(dirname(realpath(__file__))))

_FAABRIC_BUILD_DIR = environ.get("FAABRIC_BUILD_DIR", "/build/faabric")

FAABRIC_SHARED_BUILD_DIR = join(_FAABRIC_BUILD_DIR, "shared")
FAABRIC_STATIC_BUILD_DIR = join(_FAABRIC_BUILD_DIR, "static")

FAABRIC_INSTALL_PREFIX = join(_FAABRIC_BUILD_DIR, "install")


def get_version():
    ver_file = join(PROJ_ROOT, "VERSION")

    with open(ver_file, "r") as fh:
        version = fh.read()

    version = version.strip()
    return version
