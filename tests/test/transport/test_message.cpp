#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <thread>

#include <faabric/transport/Message.h>
#include <faabric/transport/common.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

using namespace faabric::transport;

namespace tests {

TEST_CASE("Test message moving", "[transport]")
{
    size_t msgSize = 100;

    faabric::transport::Message m(msgSize);
    uint8_t* dataPtr = m.udata();

    // Set some data
    dataPtr[0] = 1;
    dataPtr[1] = 2;
    dataPtr[2] = 3;

    // Create moved copy
    faabric::transport::Message mB(std::move(m));

    // Check we can still access the pointer
    REQUIRE(mB.udata() == dataPtr);

    REQUIRE(dataPtr[0] == 1);
    REQUIRE(dataPtr[1] == 2);
    REQUIRE(dataPtr[2] == 3);
}
}
