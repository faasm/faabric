from invoke import Collection

from . import call
from . import dev
from . import docker
from . import docs
from . import examples
from . import git
from . import format_code
from . import tests

ns = Collection(
    call,
    dev,
    docker,
    docs,
    examples,
    git,
    format_code,
    tests,
)
