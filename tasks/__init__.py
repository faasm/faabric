from invoke import Collection

from . import call
from . import container
from . import dev
from . import docs
from . import examples
from . import git

ns = Collection(
    call,
    container,
    dev,
    docs,
    examples,
    git,
)
