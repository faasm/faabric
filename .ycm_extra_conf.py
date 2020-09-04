from os.path import dirname, realpath, join, exists

_PROJ_ROOT = dirname(realpath(__file__))


def Settings(**kwargs):
    venv_interpreter = join(_PROJ_ROOT, "venv", "bin", "python")

    if not exists(venv_interpreter):
        parent_root = dirname(dirname(_PROJ_ROOT))
        venv_interpreter = join(parent_root, "venv", "bin", "python")

    return {"interpreter_path": venv_interpreter}
