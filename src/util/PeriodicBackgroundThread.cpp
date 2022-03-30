#include <faabric/util/PeriodicBackgroundThread.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::util {

void PeriodicBackgroundThread::start(int wakeUpPeriodSecondsIn)
{
    wakeUpPeriodSeconds = wakeUpPeriodSecondsIn;

    workThread = std::make_unique<std::jthread>([&](std::stop_token st) {
        while (!st.stop_requested()) {
            faabric::util::UniqueLock lock(mx);

            if (st.stop_requested()) {
                break;
            }

            std::cv_status returnVal = timeoutCv.wait_for(
              lock, std::chrono::milliseconds(wakeUpPeriodSeconds * 1000));

            // If we hit the timeout it means we have not been notified to
            // stop. Thus we can do work
            if (returnVal == std::cv_status::timeout) {
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

    faabric::util::UniqueLock lock(mx);
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
