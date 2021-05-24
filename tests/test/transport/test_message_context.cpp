#include <catch.hpp>

#include <faabric/transport/MessageContext.h>

using namespace faabric::transport;

namespace tests {
TEST_CASE("Test global message context", "[transport]")
{
    // Get message context
    MessageContext& context = getGlobalMessageContext();

    // Context not shut down
    REQUIRE(!context.isContextShutDown);

    // Close message context
    REQUIRE_NOTHROW(context.close());

    // Context is shut down
    REQUIRE(context.isContextShutDown);

    // Get message context again, lazy-initialise it
    MessageContext& newContext = getGlobalMessageContext();

    // Context not shut down
    REQUIRE(!newContext.isContextShutDown);

    newContext.close();
}
}
