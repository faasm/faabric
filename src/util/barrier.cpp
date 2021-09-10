#include <faabric/util/barrier.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::util {

std::shared_ptr<Barrier> Barrier::create(
  int count,
  std::function<void()> completionFunctionIn,
  int timeoutMs)
{
    return std::make_shared<Barrier>(count, completionFunctionIn, timeoutMs);
}

std::shared_ptr<Barrier> Barrier::create(int count, int timeoutMs)
{
    return std::make_shared<Barrier>(
      count, []() {}, timeoutMs);
}

Barrier::Barrier(int countIn,
                 std::function<void()> completionFunctionIn,
                 int timeoutMsIn)
  : count(countIn)
  , completionFunction(completionFunctionIn)
  , timeoutMs(timeoutMsIn)
{}

void Barrier::wait()
{
    UniqueLock lock(mx);

    visits++;
    int phaseCompletionVisits = currentPhase * count;

    if (visits == phaseCompletionVisits) {
        completionFunction();
        currentPhase++;
        cv.notify_all();
    } else {
        auto timePoint = std::chrono::system_clock::now() +
                         std::chrono::milliseconds(timeoutMs);

        if (!cv.wait_until(lock, timePoint, [this, phaseCompletionVisits] {
                return visits >= phaseCompletionVisits;
            })) {
            std::string msg =
              fmt::format("Barrier timed out ({}ms)", timeoutMs);
            SPDLOG_ERROR(msg);
            throw std::runtime_error("Barrier timed out");
        }
    }
}
}
