#include <faabric/util/latch.h>
#include <faabric/util/locks.h>

namespace faabric::util {
Latch::Latch(int countIn, int timeoutMsIn)
  : count(countIn)
  , timeoutMs(timeoutMsIn)
{}

void Latch::wait()
{
    UniqueLock lock(mx);

    waiters++;

    if (waiters > count) {
        throw std::runtime_error("Latch already used");
    }

    if (waiters == count) {
        cv.notify_all();
    } else {
        auto timePoint = std::chrono::system_clock::now() +
                         std::chrono::milliseconds(timeoutMs);

        if (!cv.wait_until(lock, timePoint, [&] { return waiters >= count; })) {
            throw std::runtime_error("latch timed out");
        }
    }
}
}
