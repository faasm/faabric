#pragma once

#include <faabric/util/PeriodicBackgroundThread.h>

#include <condition_variable>
#include <mutex>
#include <thread>

using namespace faabric::util;

namespace faabric::scheduler {

/**
 * Background thread that, every wake up period, will check if there
 * are migration opportunities for in-flight apps that have opted in to
 * being checked for migrations.
 */
class FunctionMigrationThread : public PeriodicBackgroundThread
{
  public:
    void doWork() override;
};
}
