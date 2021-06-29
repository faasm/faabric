#include <faabric/util/barrier.h>
#include <faabric/util/locks.h>

namespace faabric::util {
Barrier::Barrier(int count, int timeoutMsIn)
  : threadCount(count)
  , slotCount(count)
  , uses(0)
  , timeoutMs(timeoutMsIn)
{}

void Barrier::wait()
{
    {
        UniqueLock lock(mx);
        int usesCopy = uses;

        slotCount--;
        if (slotCount == 0) {
            uses++;
            // Checks for overflow
            if (uses < 0) {
                throw std::runtime_error("Barrier was used too many times");
            }
            slotCount = threadCount;
            cv.notify_all();
        } else {
            auto timePoint = std::chrono::system_clock::now() +
                             std::chrono::milliseconds(timeoutMs);
            bool waitRes =
              cv.wait_until(lock, timePoint, [&] { return usesCopy < uses; });
            if (!waitRes) {
                throw std::runtime_error("Barrier timed out");
            }
        }
    }
}

int Barrier::getSlotCount()
{
    return slotCount;
}

int Barrier::getUseCount()
{
    return uses;
}

}
