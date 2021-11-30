#include <faabric/util/latch.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::util {

std::shared_ptr<Latch> Latch::create(int count, int timeoutMs)
{
    return std::make_shared<Latch>(count, timeoutMs);
}

Latch::Latch(int countIn, int timeoutMsIn)
  : count(countIn)
  , timeoutMs(timeoutMsIn)
{}

void Latch::wait()
{
    // Keep the this shared_ptr alive to prevent heap-use-after-free
    std::shared_ptr<Latch> _keepMeAlive = shared_from_this();
    UniqueLock lock(mx);

    waiters++;

    if (waiters > count) {
        SPDLOG_ERROR("Latch already used: {} > {}", waiters, count);
        throw std::runtime_error("Latch already used");
    }

    if (waiters == count) {
        cv.notify_all();
    } else {
        auto timePoint = std::chrono::system_clock::now() +
                         std::chrono::milliseconds(timeoutMs);

        if (!cv.wait_until(lock, timePoint, [&] { return waiters >= count; })) {
            throw std::runtime_error("Latch timed out");
        }
    }
}
}
