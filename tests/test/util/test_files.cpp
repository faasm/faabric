#include "faabric/util/bytes.h"
#include "faabric/util/config.h"
#include <catch2/catch.hpp>

#include <faabric/util/files.h>

using namespace faabric::util;

namespace tests {

TEST_CASE("Test writing to a file", "[util]")
{
    std::string dummyFile = "/tmp/faasmTest1.txt";

    // Write to the file
    std::vector<uint8_t> bytesIn = { 0, 1, 2, 10, 20 };
    faabric::util::writeBytesToFile(dummyFile, bytesIn);

    // Read in
    std::vector<uint8_t> actual = faabric::util::readFileToBytes(dummyFile);

    // Check they match
    REQUIRE(actual == bytesIn);
}
}
