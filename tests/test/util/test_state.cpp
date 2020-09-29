#include <catch/catch.hpp>
#include <faabric/util/state.h>

using namespace faabric::util;

namespace tests {

TEST_CASE("Test creating key for user", "[util]")
{
    REQUIRE(faabric::util::keyForUser("foo", "bar") == "foo_bar");
}
}