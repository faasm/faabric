from invoke import task
from os import environ, listdir
from os.path import join
from subprocess import run
from tasks.util.env import FAABRIC_STATIC_BUILD_DIR, PROJ_ROOT

IS_CI = "HOST_TYPE" in environ and environ["HOST_TYPE"] == "ci"

TEST_ENV = {
    "LOG_LEVEL": "info",
    "PLANNER_HOST": "localhost",
    "REDIS_QUEUE_HOST": "redis",
    "REDIS_STATE_HOST": "redis",
    "TERM": "xterm-256color",
    "ASAN_OPTIONS": "verbosity=1:halt_on_error=1:",
    "LSAN_OPTIONS": "suppressions={}/leak-sanitizer-ignorelist.txt".format(
        PROJ_ROOT
    ),
    "TSAN_OPTIONS": " ".join(
        [
            "verbosity=1 halt_on_error=1",
            "suppressions={}/thread-sanitizer-ignorelist.txt".format(
                PROJ_ROOT
            ),
            "history_size=4",
        ]
    ),
    "UBSAN_OPTIONS": "print_stacktrace=1:halt_on_error=1",
}

# If executing in CI, give us some extra CPU cores to run the tests
if IS_CI:
    TEST_ENV["OVERRIDE_CPU_COUNT"] = "5"


@task(default=True)
def tests(
    ctx,
    test_case=None,
    test_file=None,
    test_dir=None,
    abort=False,
    debug=False,
    trace=False,
    repeats=1,
):
    """
    Run the C++ unit tests

    When running this task with no arguments, the whole test suite will be
    executed. You can specify the name of the test to run by passing the
    --test-case variable. Additionally, you may also specify a filename to
    run (--filename) or a directory (--directory)
    """
    tests_cmd = [
        join(FAABRIC_STATIC_BUILD_DIR, "bin", "faabric_tests"),
        "--use-colour yes",
        "--abort" if abort else "",
    ]

    # Allow specific environment variables
    if "LLVM_PROFILE_FILE" in environ:
        TEST_ENV["LLVM_PROFILE_FILE"] = environ["LLVM_PROFILE_FILE"]

    if debug:
        TEST_ENV["LOG_LEVEL"] = "debug"

    if trace:
        TEST_ENV["LOG_LEVEL"] = "trace"

    if test_case:
        tests_cmd.append('"{}"'.format(test_case))
    elif test_file:
        tests_cmd.append("--filenames-as-tags [#{}]".format(test_file))
    elif test_dir:
        tag_str = "--filenames-as-tags "
        for file_name in listdir(join(PROJ_ROOT, "tests", "test", test_dir)):
            tag_str += "[#{}],".format(file_name.split(".")[0])
        tests_cmd.append(tag_str[:-1])

    tests_cmd = " ".join(tests_cmd)
    for i in range(repeats):
        run(tests_cmd, shell=True, check=True, cwd=PROJ_ROOT, env=TEST_ENV)
