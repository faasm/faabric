#include <catch.hpp>
#include <faabric_utils.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>

using namespace faabric::scheduler;

namespace tests {

class DistributedSyncTestFixture
{};

TEST_CASE_METHOD(DistributedSyncTestFixture,
                 "Test distributed mutexes",
                 "[scheduler][sync]")
{}
}
