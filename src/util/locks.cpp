#include <faabric/util/locks.h>

namespace faabric::util {

FlagWaiter::FlagWaiter(int timeoutMsIn)
  : timeoutMs(timeoutMsIn)
{}

void FlagWaiter::waitOnFlag()
{
    // Keep the this shared_ptr alive to prevent heap-use-after-free
    std::shared_ptr<FlagWaiter> _keepMeAlive = shared_from_this();
    // Check
    if (flag.load()) {
        return;
    }

    // Wait for flag to be set
    UniqueLock lock(flagMx);
    if (!cv.wait_for(lock, std::chrono::milliseconds(timeoutMs), [this] {
            return flag.load();
        })) {

        SPDLOG_ERROR("Timed out waiting for flag");
        throw std::runtime_error("Timed out waiting for flag");
    }
}

void FlagWaiter::setFlag(bool value)
{
    // Keep the this shared_ptr alive to prevent heap-use-after-free
    std::shared_ptr<FlagWaiter> _keepMeAlive = shared_from_this();
    UniqueLock lock(flagMx);
    flag.store(value);
    cv.notify_all();
}
}
