#include <catch.hpp>

#include "faabric_utils.h"

#include <faabric/redis/Redis.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/environment.h>

using namespace scheduler;
using namespace redis;

namespace tests {
TEST_CASE("Test scheduler clear-up", "[scheduler]")
{
    cleanFaabric();

    faabric::Message call;
    call.set_user("some user");
    call.set_function("some function");
    std::string funcSet;

    std::string thisHost = faabric::util::getSystemConfig().endpointHost;
    Redis& redis = Redis::getQueue();

    Scheduler s;
    funcSet = s.getFunctionWarmSetName(call);

    // Check initial set-up
    REQUIRE(redis.sismember(AVAILABLE_HOST_SET, thisHost));
    REQUIRE(!redis.sismember(funcSet, thisHost));

    // Call the function and check it's added to the function's warm set
    s.callFunction(call);

    REQUIRE(redis.sismember(AVAILABLE_HOST_SET, thisHost));
    REQUIRE(redis.sismember(funcSet, thisHost));

    // Run shutdown
    s.shutdown();

    // After clear-up has run this host should no longer be part of either set
    REQUIRE(!redis.sismember(AVAILABLE_HOST_SET, thisHost));
    REQUIRE(!redis.sismember(funcSet, thisHost));
}
}
