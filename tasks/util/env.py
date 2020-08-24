from os.path import dirname, realpath, join, expanduser

HOME_DIR = expanduser("~")
PROJ_ROOT = dirname(dirname(dirname(realpath(__file__))))
ANSIBLE_ROOT = join(PROJ_ROOT, "ansible")


def get_version():
    ver_file = join(PROJ_ROOT, "VERSION")

    with open(ver_file, "r") as fh:
        version = fh.read()

    version = version.strip()
    return version