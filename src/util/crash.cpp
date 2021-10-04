#include <faabric/util/crash.h>
#include <faabric/util/logging.h>

#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>

const std::string_view ABORT_MSG = "Caught stack backtrace:\n";
constexpr int TEST_SIGNAL = 12341234;

// Must be async-signal-safe - don't call allocating functions
void crashHandler(int sig) noexcept
{
    std::array<void*, 32> stackPtrs;
    size_t filledStacks = backtrace(stackPtrs.data(), stackPtrs.size());
    if (sig != TEST_SIGNAL) {
        write(STDERR_FILENO, ABORT_MSG.data(), ABORT_MSG.size());
    }
    backtrace_symbols_fd(stackPtrs.data(),
                         std::min(filledStacks, stackPtrs.size()),
                         STDERR_FILENO);
    if (sig != TEST_SIGNAL) {
        signal(sig, SIG_DFL);
        raise(sig);
        exit(1);
    }
    return;
}

namespace faabric::util {

void setUpCrashHandler()
{
    fputs("Testing crash handler backtrace:\n", stderr);
    fflush(stderr);
    crashHandler(TEST_SIGNAL);
    SPDLOG_INFO("Installing crash handler");
    for (auto signo : { SIGSEGV, SIGABRT, SIGILL, SIGFPE }) {
        if (signal(signo, &crashHandler) == SIG_ERR) {
            SPDLOG_WARN("Couldn't install handler for signal {}", signo);
        } else {
            SPDLOG_INFO("Installed handler for signal {}", signo);
        }
    }
}

}
