from invoke import Collection

from . import build
from . import container
from . import dev

ns = Collection(
    build,
    container,
    dev,
)

