#include "faabric/util/bytes.h"
#include "faabric/util/config.h"
#include <catch.hpp>

#include <faabric/util/http.h>
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

struct LargeDataHandler : public Http::Handler
{
    HTTP_PROTOTYPE(LargeDataHandler)

    void onRequest(const Http::Request&, Http::ResponseWriter writer) override
    {
        std::string largeContents(1024 * 1024, 'a');
        writer.send(Http::Code::Ok, largeContents);
    }
};

TEST_CASE("Test reading data from a URL", "[util]")
{
    const Pistache::Address address("localhost", Pistache::Port(0));

    std::shared_ptr<Http::Handler> handler;
    SECTION("Small data") { handler = Http::make_handler<DummyHandler>(); }
//    SECTION("Big data") { handler = Http::make_handler<LargeDataHandler>(); }

    Http::Endpoint server(address);
    auto flags = Tcp::Options::ReuseAddr;
    auto serverOpts = Http::Endpoint::options().flags(flags);
    server.init(serverOpts);
    server.setHandler(handler);
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

    SECTION("500 error")
    {
        url = "httpstat.us/500";
        expectedMessage = "Error reading file from httpstat.us/500 (500)";
    }
    SECTION("203 code")
    {
        url = "httpstat.us/203";
        expectedMessage = "Error reading file from httpstat.us/203 (203)";
    }

    bool exceptionThrown = false;
    try {
        faabric::util::readFileFromUrl(url);
    } catch (faabric::util::FaabricHttpException& ex) {
        exceptionThrown = true;
        REQUIRE(ex.what() == expectedMessage);
    }

    REQUIRE(exceptionThrown);
}
}
