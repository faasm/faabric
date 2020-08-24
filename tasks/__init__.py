from invoke import Collection

from . import build
from . import containers

ns = Collection(
    build,
    containers,
)

