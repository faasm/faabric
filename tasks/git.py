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
def bump(ctx, patch=False, minor=False, major=False):
    """
    Bump the code version by --patch, --minor, or --major
    """
    old_ver = get_version()
    new_ver_parts = old_ver.split(".")

    if patch:
        idx = 2
    elif minor:
        idx = 1
    elif major:
        idx = 0
    else:
        raise RuntimeError("Must set one in: --[patch,minor,major]")

    # Change the corresponding idx
    new_ver_parts[idx] = str(int(new_ver_parts[idx]) + 1)

    # Zero-out the following version numbers (i.e. lower priority). This is
    # because if we tag a new major release, we want to zero-out the minor
    # and patch versions (e.g. 0.2.0 comes after 0.1.9)
    for next_idx in range(idx + 1, 3):
        new_ver_parts[next_idx] = "0"

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
    git_cmd = (
        "git log --pretty=format:'%d,%s,%as' {}...v{}".format(
            get_release().tag_name, get_version()
        ),
    )
    commits = (
        run(git_cmd, shell=True, capture_output=True, cwd=PROJ_ROOT)
        .stdout.decode("utf-8")
        .split("\n")
    )
    body = "Here is what has changed since last release:\n"

    def make_tag_header(body, tag, date):
        tag = tag.split(" ")[2][:-1]
        body += "\n## [{}] - {}\n".format(tag, date)
        return body

    def get_commit_parts(commit):
        first_comma = commit.find(",")
        last_comma = commit.rfind(",")
        tag_end = first_comma
        msg_start = first_comma + 1
        msg_end = last_comma
        date_start = last_comma + 1
        tag = commit[0:tag_end]
        msg = commit[msg_start:msg_end]
        date = commit[date_start:]
        return tag, msg, date

    for commit in commits:
        tag, msg, date = get_commit_parts(commit)
        if tag:
            body = make_tag_header(body, tag, date)
        body += "* {}\n".format(msg)
    return body.strip()


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
