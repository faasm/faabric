from invoke import Collection

from . import build
from . import call
from . import container
from . import dev

ns = Collection(
    build,
    call,
    container,
    dev,
)

