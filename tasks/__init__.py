from invoke import Collection

from . import call
from . import container
from . import dev
from . import git

ns = Collection(
    call,
    container,
    dev,
    git,
)
