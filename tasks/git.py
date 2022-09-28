from github import Github
from invoke import task
from tasks.util.env import get_faabric_config, get_version, PROJ_ROOT
from subprocess import run, PIPE, STDOUT

VERSIONED_FILES = [
    ".env",
    ".github/workflows/tests.yml",
    "VERSION",
]


def get_tag_name(version):
    return "v{}".format(version)


@task
def bump(ctx, ver=None):
    """
    Bump code version
    """
    old_ver = get_version()

    if ver:
        new_ver = ver
    else:
        # Just bump the last minor version part
        new_ver_parts = old_ver.split(".")
        new_ver_minor = int(new_ver_parts[-1]) + 1
        new_ver_parts[-1] = str(new_ver_minor)
        new_ver = ".".join(new_ver_parts)

    # Replace version in all files
    for f in VERSIONED_FILES:
        sed_cmd = "sed -i 's/{}/{}/g' {}".format(old_ver, new_ver, f)
        run(sed_cmd, shell=True, check=True)


@task
def tag(ctx, force=False):
    """
    Creates git tag from the current tree
    """
    git_tag = get_tag_name(get_version())
    run(
        "git tag {} {}".format("--force" if force else "", git_tag),
        shell=True,
        check=True,
        cwd=PROJ_ROOT,
    )

    run(
        "git push origin {} {}".format("--force" if force else "", git_tag),
        shell=True,
        check=True,
        cwd=PROJ_ROOT,
    )


def is_git_submodule():
    git_cmd = "git rev-parse --show-superproject-working-tree"
    result = run(git_cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    return result.stdout.decode("utf-8") != ""


def get_github_instance():
    conf = get_faabric_config()

    if not conf.has_section("Github") or not conf.has_option(
        "Github", "access_token"
    ):
        print("Must set up Github config with access token")

    token = conf["Github"]["access_token"]
    g = Github(token)
    return g


def get_repo():
    g = get_github_instance()
    return g.get_repo("faasm/faabric")


def get_release():
    r = get_repo()
    rels = r.get_releases()

    return rels[0]


def get_release_body():
    """
    Generate body for release with detailed changelog
    """
    if is_git_submodule():
        docker_cmd = [
            "docker run -t -v",
            "{}/..:/app/".format(PROJ_ROOT),
            "orhunp/git-cliff:latest",
            "--config ./faabric/cliff.toml",
            "--repository ./faabric",
            "{}..v{}".format(get_release().tag_name, get_version()),
        ]
    else:
        docker_cmd = [
            "docker run -t -v",
            "{}:/app/".format(PROJ_ROOT),
            "orhunp/git-cliff:latest",
            "--config cliff.toml",
            "--repository .",
            "{}..v{}".format(get_release().tag_name, get_version()),
        ]

    cmd = " ".join(docker_cmd)
    print("Generating release body...")
    print(cmd)
    result = run(cmd, shell=True, stdout=PIPE, stderr=PIPE)

    return result.stdout.decode("utf-8")


@task
def release_create(ctx):
    """
    Create a draft release on Github
    """
    # Work out the tag
    faabric_ver = get_version()
    tag_name = get_tag_name(faabric_ver)

    # Create a release in github from this tag
    r = get_repo()
    r.create_git_release(
        tag_name,
        "Faabric {}".format(faabric_ver),
        get_release_body(),
        draft=True,
    )

    print("You may now review the draft release in:")
    print("https://github.com/faasm/faabric/releases")


@task
def release_publish(ctx):
    """
    Publish the draft release
    """
    rel = get_release()
    rel.update_release(rel.title, rel.raw_data["body"], draft=False)
