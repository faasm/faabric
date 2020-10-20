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

TEST_CASE("Test reading from a URL", "[util]")
{
    auto conf = faabric::util::getSystemConfig();

    // Skip if we're in CI
    if (conf.hostType != "ci") {
        std::string localPath = "/usr/local/code/faabric/LICENSE.md";
        std::string url =
          "https://raw.githubusercontent.com/faasm/faabric/master/LICENSE.md";

        std::vector<uint8_t> expectedBytes =
          faabric::util::readFileToBytes(localPath);
        std::vector<uint8_t> actualBytes = faabric::util::readFileFromUrl(url);

        REQUIRE(actualBytes == expectedBytes);
    }
}

TEST_CASE("Test reading from bad URLs", "[util]")
{
    std::string url;
    std::string expectedMessage;

    SECTION("Invalid URL")
    {
        url = "https://www.aklskafkjdfkh.com/foo.txt";
        expectedMessage = "Unable to get file due to curl error " + url;
    }
    SECTION("500 error")
    {
        url = "https://httpstat.us/500";
        expectedMessage = "Unable to get file " + url + " response code: 500";
    }
    SECTION("203 code")
    {
        url = "https://httpstat.us/203";
        expectedMessage = "Unable to get file " + url + " response code: 203";
    }

    bool exceptionThrown = false;
    try {
        faabric::util::readFileFromUrl(url);
    } catch (faabric::util::FileNotFoundAtUrlException& ex) {
        exceptionThrown = true;
    }
    REQUIRE(exceptionThrown);
}
}
