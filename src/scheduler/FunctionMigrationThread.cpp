#include <faabric/scheduler/FunctionMigrationThread.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
void FunctionMigrationThread::start(int wakeUpPeriodSecondsIn)
{
    wakeUpPeriodSeconds = wakeUpPeriodSecondsIn;

    // Main work loop
    workThread = std::make_unique<std::thread>([&] {
        // As we only check for migration opportunities every (possibly user-
        // defined) timeout, we also support stopping the main thread through
        // a condition variable.
        while (!isShutdown.load(std::memory_order_acquire)) {
            faabric::util::UniqueLock lock(mx);

            if (isShutdown.load(std::memory_order_acquire)) {
                break;
            }

            std::cv_status returnVal = mustStopCv.wait_for(
              lock, std::chrono::milliseconds(wakeUpPeriodSeconds * 1000));

            // If we hit the timeout it means we have not been notified to
            // reset or stop. Thus we check for migration oportunities.
            if (returnVal == std::cv_status::timeout) {
                SPDLOG_DEBUG("Checking for migration oportunities");
                faabric::scheduler::getScheduler()
                  .checkForMigrationOpportunities();
            }
        };

        SPDLOG_DEBUG("Exiting main function migration thread loop");
    });
}

void FunctionMigrationThread::stop()
{
    if (workThread == nullptr) {
        return;
    }

    {
        faabric::util::UniqueLock lock(mx);

        // We set the flag _before_ we notify and after we acquire the lock.
        // Therefore, either we check the flag (before going to sleep) or are
        // woken by the notification.
        isShutdown.store(true, std::memory_order_release);
        mustStopCv.notify_one();
    }

    if (workThread->joinable()) {
        workThread->join();
    }
}
}
