from invoke import Collection

from . import build
from . import container

ns = Collection(
    build,
    container,
)

