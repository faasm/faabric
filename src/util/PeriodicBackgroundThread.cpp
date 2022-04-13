#include <faabric/util/PeriodicBackgroundThread.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::util {

void PeriodicBackgroundThread::start(int intervalSecondsIn)
{
    intervalSeconds = intervalSecondsIn;
    SPDLOG_DEBUG("Starting periodic background thread with interval {}s",
                 intervalSeconds);

    workThread = std::make_unique<std::jthread>([&](std::stop_token st) {
        while (!st.stop_requested()) {
            faabric::util::UniqueLock lock(mx);
            if (st.stop_requested()) {
                break;
            }

            bool isStopped = timeoutCv.wait_for(
              lock, std::chrono::milliseconds(intervalSeconds * 1000), [&st] {
                  return st.stop_requested();
              });

            // If we hit the timeout it means we have not been notified to
            // stop. Thus we can do work
            if (!isStopped) {
                doWork();
            }
        };

        SPDLOG_DEBUG("Exiting periodic background thread");
    });
}

void PeriodicBackgroundThread::tidyUp()
{
    // Hook for subclasses
}

void PeriodicBackgroundThread::stop()
{
    if (workThread == nullptr) {
        return;
    }

    SPDLOG_TRACE("Stopping periodic background thread");

    workThread->request_stop();
    timeoutCv.notify_one();

    // Join to make sure no background tasks are running
    if (workThread->joinable()) {
        workThread->join();
    }

    // Hook into tidy up function
    tidyUp();
}
}
