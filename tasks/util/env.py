from os import environ
from os.path import dirname, exists, realpath, join, expanduser
import configparser

HOME_DIR = expanduser("~")
PROJ_ROOT = dirname(dirname(dirname(realpath(__file__))))

_FAABRIC_BUILD_DIR = environ.get("FAABRIC_BUILD_DIR", "/build/faabric")

FAABRIC_SHARED_BUILD_DIR = join(_FAABRIC_BUILD_DIR, "shared")
FAABRIC_STATIC_BUILD_DIR = join(_FAABRIC_BUILD_DIR, "static")

FAABRIC_INSTALL_PREFIX = join(_FAABRIC_BUILD_DIR, "install")

FAABRIC_CONFIG_FILE = join(PROJ_ROOT, "faabric.ini")

FAABRIC_CONAN_CACHE = join(PROJ_ROOT, "conan-cache")

CR_NAME = "ghcr.io/faasm"

# This LLVM version is for the LLVM that we use to compile regular C/C++ code
# to x86. For the LLVM version to cross-compile code to WebAssembly see
# faasm/cpp/faasmtools/env.py. Ideally, both versions will be in sync but it
# is not strictly necessary.
LLVM_VERSION = "17.0.6"
LLVM_VERSION_MAJOR = LLVM_VERSION.split(".")[0]


def get_version():
    ver_file = join(PROJ_ROOT, "VERSION")

    with open(ver_file, "r") as fh:
        version = fh.read()

    version = version.strip()
    return version


def get_faabric_config():
    config = configparser.ConfigParser()
    if not exists(FAABRIC_CONFIG_FILE):
        print("Creating config file at {}".format(FAABRIC_CONFIG_FILE))

        with open(FAABRIC_CONFIG_FILE, "w") as fh:
            config.write(fh)
    else:
        config.read(FAABRIC_CONFIG_FILE)

    return config
