from invoke import Collection

from . import call
from . import container
from . import dev
from . import docs
from . import examples
from . import git
from . import mpi_native

ns = Collection(
    call,
    container,
    dev,
    docs,
    examples,
    git,
    mpi_native,
)
