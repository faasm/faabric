#include <catch2/catch.hpp>

#include <faabric/util/batch.h>
#include <faabric/util/func.h>

using namespace faabric::util;

namespace tests {
TEST_CASE("Test batch exec factory", "[util]")
{
    int nMessages = 4;
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      batchExecFactory("demo", "echo", nMessages);

    REQUIRE(req->messages().size() == nMessages);

    REQUIRE(req->appid() > 0);

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

    // An empty user deems a BER invalid
    SECTION("Empty user")
    {
        isBerValid = false;
        ber->mutable_messages(0)->set_user("");
    }

    // An empty function (or a different function name) is admissible to allow
    // for function chaining by name
    SECTION("Empty function")
    {
        isBerValid = false;
        ber->mutable_messages(0)->set_function("");
    }

    // A user mismatch between the messages deems a BER invalid
    SECTION("User mismatch")
    {
        isBerValid = false;
        ber->mutable_messages(1)->set_user("foo");
    }

    // A function mismatch between the messages is acceptable to allow for
    // function chaining
    SECTION("Function mismatch")
    {
        isBerValid = true;
        ber->mutable_messages(1)->set_function("foo");
    }

    // BERs constructed with the default factory are valid
    SECTION("Valid BER")
    {
        isBerValid = true;
    }

    REQUIRE(isBerValid == isBatchExecRequestValid(ber));
}

TEST_CASE("Test updating the app ID of a BER")
{
    int nMessages = 4;
    std::shared_ptr<faabric::BatchExecuteRequest> ber =
      batchExecFactory("demo", "echo", nMessages);

    // By default the BER is valid
    REQUIRE(isBatchExecRequestValid(ber));

    int newAppId = 1337;
    updateBatchExecAppId(ber, newAppId);
    REQUIRE(isBatchExecRequestValid(ber));
    REQUIRE(ber->appid() == newAppId);
}

TEST_CASE("Test updating the group ID of a BER")
{
    int nMessages = 4;
    std::shared_ptr<faabric::BatchExecuteRequest> ber =
      batchExecFactory("demo", "echo", nMessages);

    // By default the BER is valid
    REQUIRE(isBatchExecRequestValid(ber));

    int newGroupId = 1337;
    updateBatchExecGroupId(ber, newGroupId);
    REQUIRE(isBatchExecRequestValid(ber));
    REQUIRE(ber->groupid() == newGroupId);
}

TEST_CASE("Test BER status factory")
{
    int appId;
    std::shared_ptr<faabric::BatchExecuteRequestStatus> berStatus = nullptr;

    // A BER status can be constructed with an appId
    SECTION("Constructor with app id")
    {
        appId = 1337;
        berStatus = faabric::util::batchExecStatusFactory(appId);
    }

    // A BER status can also be constructed from a BER
    SECTION("Constructor from a BER")
    {
        auto ber = faabric::util::batchExecFactory("foo", "bar", 1);
        appId = ber->appid();
        berStatus = faabric::util::batchExecStatusFactory(ber);
    }

    // It will have the same appId
    REQUIRE(berStatus->appid() == appId);

    // And the finished flag will be set to false
    REQUIRE(berStatus->finished() == false);
}

TEST_CASE("Test getting the number of finished messages in a BER")
{
    int numTotalMsgs = 4;
    int expectedNumFinishedMsgs = 0;
    auto ber = faabric::util::batchExecFactory("foo", "bar", numTotalMsgs);
    auto berStatus = faabric::util::batchExecStatusFactory(ber);
    for (int i = 0; i < ber->messages_size(); i++) {
        *berStatus->add_messageresults() = *ber->mutable_messages(i);
        berStatus->mutable_messageresults(i)->set_returnvalue(0);
    }

    SECTION("All messages have finished")
    {
        expectedNumFinishedMsgs = numTotalMsgs;
    }

    SECTION("One message has been migrated")
    {
        expectedNumFinishedMsgs = 3;
        berStatus->mutable_messageresults(0)->set_returnvalue(
          MIGRATED_FUNCTION_RETURN_VALUE);
    }

    REQUIRE(faabric::util::getNumFinishedMessagesInBatch(berStatus) ==
            expectedNumFinishedMsgs);
}
}
