from invoke import Collection

from . import call
from . import container
from . import dev
from . import examples
from . import git

ns = Collection(
    call,
    container,
    dev,
    examples,
    git,
)
