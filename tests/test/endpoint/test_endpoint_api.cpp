#include <catch.hpp>

#include "faabric_utils.h"

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/endpoint/FaabricEndpointHandler.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/json.h>
#include <faabric/util/macros.h>

using namespace Pistache;

namespace tests {
TEST_CASE_METHOD(SchedulerTestFixture, "Test request to endpoint", "[endpoint]")
{
    faabric::endpoint::FaabricEndpoint endpoint;

    std::thread serverThread([&endpoint]() { endpoint.start(); });

    SLEEP_MS(5000);

    endpoint.stop();
}
}
