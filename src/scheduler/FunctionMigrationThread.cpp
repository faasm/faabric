#include <faabric/scheduler/FunctionMigrationThread.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
void FunctionMigrationThread::start()
{
    /*
    auto& conf = faabric::util::getSystemConfig();
    if (conf.funcMigration == "off") {
        SPDLOG_INFO(
          "Not starting migration thread as it is not enabled in the config");
        return;
    }

    if (conf.migrationCheckPeriod <= 0) {
        SPDLOG_ERROR("Starting function migration server with non-positive "
                     "check period: {}",
                     conf.migrationCheckPeriod);
        throw std::runtime_error(
          "Migration server received wrong check period");
    }
    */

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

            // Work out how much we need to sleep for. Note that the scheduler
            // returns the time in seconds.
            int timeToSleep = faabric::scheduler::getScheduler()
                                .getFunctionMigrationServerSleepTime();

            std::cv_status returnVal = mustStopCv.wait_for(
              lock, std::chrono::milliseconds(timeToSleep * 1000));

            // If we hit the timeout it means we have not been notified to
            // reset or stop. Thus we check for migration oportunities.
            if (returnVal == std::cv_status::timeout) {
                SPDLOG_INFO("Checking for migration oportunities");
                faabric::scheduler::getScheduler()
                  .checkForMigrationOpportunities();
            }
        };

        SPDLOG_DEBUG("Exiting main function migration thread loop");
    });
}

void FunctionMigrationThread::stop()
{
    /*
    auto& conf = faabric::util::getSystemConfig();
    if (conf.funcMigration == "off") {
        return;
    }
    */
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
