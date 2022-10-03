from invoke import Collection

from . import call
from . import dev
from . import docker
from . import docs
from . import examples
from . import git

ns = Collection(
    call,
    dev,
    docker,
    docs,
    examples,
    git,
)
