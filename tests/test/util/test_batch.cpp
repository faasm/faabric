#include <catch2/catch.hpp>

#include <faabric/util/batch.h>

using namespace faabric::util;

namespace tests {
TEST_CASE("Test batch exec factory", "[util]")
{
    int nMessages = 4;
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      batchExecFactory("demo", "echo", nMessages);

    REQUIRE(req->messages().size() == nMessages);

    REQUIRE(req->id() > 0);

    // Expect all messages to have the same app ID by default
    int appId = req->messages().at(0).appid();
    REQUIRE(appId > 0);

    for (const auto& m : req->messages()) {
        REQUIRE(m.appid() == appId);
        REQUIRE(m.user() == "demo");
        REQUIRE(m.function() == "echo");
    }
}

TEST_CASE("Test batch. exec request sanity checks")
{
    int nMessages = 4;
    std::shared_ptr<faabric::BatchExecuteRequest> ber =
      batchExecFactory("demo", "echo", nMessages);
    bool isBerValid;

    // A null BER is invalid
    SECTION("Null BER")
    {
        isBerValid = false;
        ber = nullptr;
    }

    // An empty BER is invalid
    SECTION("Empty BER")
    {
        isBerValid = false;
        ber = std::make_shared<faabric::BatchExecuteRequest>();
    }

    // An appId mismatch between the messages deems a BER invalid
    SECTION("App ID mismatch")
    {
        isBerValid = false;
        ber->mutable_messages(1)->set_appid(1337);
    }

    // A user mismatch between the messages deems a BER invalid
    SECTION("User mismatch")
    {
        isBerValid = false;
        ber->mutable_messages(1)->set_user("foo");
    }

    // A function mismatch between the messages deems a BER invalid
    SECTION("Function mismatch")
    {
        isBerValid = false;
        ber->mutable_messages(1)->set_function("foo");
    }

    // BERs constructed with the default factory are valid
    SECTION("Valid BER") { isBerValid = true; }

    REQUIRE(isBerValid == isBatchExecRequestValid(ber));
}
}
