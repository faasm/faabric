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
    std::string url = "localhost:8081";
    faabric::endpoint::FaabricEndpoint endpoint(port, 2);

    std::thread serverThread([&endpoint]() { endpoint.start(false); });

    SLEEP_MS(1000);

    endpoint.stop();

    std::pair<int, std::string> result =
      getRequestToUrl("localhost", port, "blah");
    REQUIRE(result.first == 404);

    if (serverThread.joinable()) {
        serverThread.join();
    }
}
}
