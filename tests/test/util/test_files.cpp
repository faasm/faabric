#include "faabric/util/bytes.h"
#include "faabric/util/config.h"
#include <catch2/catch.hpp>

#include <faabric/util/files.h>
#include <pistache/client.h>
#include <pistache/endpoint.h>
#include <pistache/http.h>

using namespace faabric::util;

#define DUMMY_CONTENTS "blahblahblahblah"

namespace tests {

struct DummyHandler : public Http::Handler
{
    HTTP_PROTOTYPE(DummyHandler)

    void onRequest(const Http::Request&, Http::ResponseWriter writer) override
    {
        writer.send(Http::Code::Ok, DUMMY_CONTENTS);
    }
};

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

    // Start a dummy server
    const Pistache::Address address("localhost", Pistache::Port(0));

    Http::Endpoint server(address);
    auto flags = Tcp::Options::ReuseAddr;
    auto serverOpts = Http::Endpoint::options().flags(flags);
    server.init(serverOpts);
    server.setHandler(Http::make_handler<DummyHandler>());
    server.serveThreaded();

    const std::string url = "localhost:" + server.getPort().toString();

    std::vector<uint8_t> actualBytes = faabric::util::readFileFromUrl(url);
    std::vector<uint8_t> expectedBytes =
      faabric::util::stringToBytes(DUMMY_CONTENTS);

    REQUIRE(actualBytes == expectedBytes);

    server.shutdown();
}

TEST_CASE("Test reading from bad URLs", "[util]")
{
    std::string url;
    std::string expectedMessage;

    SECTION("Invalid URL")
    {
        url = "www.aklskafkjdfkh.com/foo.txt";
        expectedMessage = "Unable to get file due to curl error " + url;
    }
    SECTION("500 error")
    {
        url = "httpstat.us/500";
        expectedMessage = "Unable to get file " + url + " response code: 500";
    }
    SECTION("203 code")
    {
        url = "httpstat.us/203";
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
