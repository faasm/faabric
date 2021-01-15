from invoke import Collection

from . import call
from . import container
from . import dev
from . import examples
from . import git
from . import mpi

ns = Collection(
    call,
    container,
    dev,
    examples,
    git,
    mpi,
)
