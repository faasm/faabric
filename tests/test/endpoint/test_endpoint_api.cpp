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
    int port = 8081;
    faabric::endpoint::FaabricEndpoint endpoint(port, 2);

    std::thread serverThread([&endpoint]() { endpoint.start(false); });

    SLEEP_MS(1000);

    std::pair<int, std::string> result = submitGetRequestToUrl(LOCALHOST, port);
    REQUIRE(result.first == 500);

    endpoint.stop();

    if (serverThread.joinable()) {
        serverThread.join();
    }
}
}
