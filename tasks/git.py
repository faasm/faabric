from invoke import task
from tasks.util.env import get_version, PROJ_ROOT
from subprocess import run, PIPE, STDOUT
import json


@task
def tag(ctx, force=False):
    """
    Creates git tag from the current tree
    """
    git_tag = "v{}".format(get_version())
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


def get_latest_release_tag():
    gh_url = "https://api.github.com/repos/faasm/faabric/releases/latest"

    curl_cmd = "curl --silent {}".format(gh_url)
    print(curl_cmd)
    result = run(curl_cmd, shell=True, stdout=PIPE, stderr=STDOUT)

    tag = json.loads(result.stdout.decode("utf-8"))["tag_name"]

    return tag


@task
def release_body(ctx, file_path="/tmp/release_body.md"):
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
            "{}..v{}".format(get_latest_release_tag(), get_version()),
        ]
    else:
        docker_cmd = [
            "docker run -t -v",
            "{}:/app/".format(PROJ_ROOT),
            "orhunp/git-cliff:latest",
            "--config cliff.toml",
            "--repository .",
            "{}..v{}".format(get_latest_release_tag(), get_version()),
        ]

    cmd = " ".join(docker_cmd)
    print(cmd)
    result = run(cmd, shell=True, stdout=PIPE, stderr=PIPE)

    with open(file_path, "w") as f:
        f.write(result.stdout.decode("utf-8"))

    print("Stored release body in temporary file:")
    print("vim {}".format(file_path))
