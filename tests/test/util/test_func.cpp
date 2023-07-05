#include <catch2/catch.hpp>

#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>

#include <boost/filesystem.hpp>

using namespace boost::filesystem;

namespace tests {
TEST_CASE("Test message factory", "[util]")
{
    const faabric::Message msg = faabric::util::messageFactory("demo", "echo");
    REQUIRE(msg.user() == "demo");
    REQUIRE(msg.function() == "echo");
    REQUIRE(msg.id() > 0);
    REQUIRE(!msg.statuskey().empty());
    REQUIRE(!msg.resultkey().empty());
}

TEST_CASE("Test message factory shared", "[util]")
{
    std::shared_ptr<faabric::Message> msg =
      faabric::util::messageFactoryShared("demo", "echo");
    REQUIRE(msg->user() == "demo");
    REQUIRE(msg->function() == "echo");
    REQUIRE(msg->id() > 0);
    REQUIRE(!msg->statuskey().empty());
    REQUIRE(!msg->resultkey().empty());
}

TEST_CASE("Test adding ids to message", "[util]")
{
    faabric::Message msgA;
    faabric::Message msgB;

    REQUIRE(msgA.id() == 0);
    REQUIRE(msgA.resultkey().empty());
    REQUIRE(msgA.statuskey().empty());
    REQUIRE(msgA.appid() == 0);

    REQUIRE(msgB.id() == 0);
    REQUIRE(msgB.resultkey().empty());
    REQUIRE(msgB.statuskey().empty());
    REQUIRE(msgB.appid() == 0);

    faabric::util::setMessageId(msgA);
    faabric::util::setMessageId(msgB);

    REQUIRE(msgA.id() > 0);
    REQUIRE(msgA.appid() > 0);
    REQUIRE(msgB.id() > 0);
    REQUIRE(msgB.appid() > 0);
    REQUIRE(msgB.id() > msgA.id());
    REQUIRE(msgB.appid() > msgA.appid());

    std::string expectedResultKeyA =
      std::string("result_" + std::to_string(msgA.id()));
    std::string expectedStatusKeyA =
      std::string("status_" + std::to_string(msgA.id()));
    REQUIRE(msgA.resultkey() == expectedResultKeyA);
    REQUIRE(msgA.statuskey() == expectedStatusKeyA);

    std::string expectedResultKeyB =
      std::string("result_" + std::to_string(msgB.id()));
    std::string expectedStatusKeyB =
      std::string("status_" + std::to_string(msgB.id()));
    REQUIRE(msgB.resultkey() == expectedResultKeyB);
    REQUIRE(msgB.statuskey() == expectedStatusKeyB);
}

TEST_CASE("Test adding ids to message with existing ids")
{
    faabric::Message msg;
    faabric::util::setMessageId(msg);

    int originalId = msg.id();
    int originalAppId = msg.appid();
    std::string originalStatusKey = msg.statuskey();
    std::string originalResultKey = msg.resultkey();

    faabric::util::setMessageId(msg);
    int afterId = msg.id();
    int afterAppId = msg.appid();
    std::string afterStatusKey = msg.statuskey();
    std::string afterResultKey = msg.resultkey();

    REQUIRE(afterId == originalId);
    REQUIRE(afterAppId == originalAppId);
    REQUIRE(afterStatusKey == originalStatusKey);
    REQUIRE(afterResultKey == originalResultKey);
}

TEST_CASE("Test timestamp added to message")
{
    faabric::Message msg;
    unsigned int msgId = 1234;

    // Epoch millis on 27/07/2020
    long baselineTimestamp = 1595862090240;
    msg.set_id(msgId);

    SECTION("Existing timestamp")
    {
        long expectedTimestamp = 999888;
        msg.set_timestamp(expectedTimestamp);

        faabric::util::setMessageId(msg);
        REQUIRE(msg.timestamp() == expectedTimestamp);
    }

    SECTION("Zero existing timestamp")
    {
        msg.set_timestamp(0);
        faabric::util::setMessageId(msg);
        REQUIRE(msg.timestamp() > baselineTimestamp);
    }

    SECTION("No existing timestamp")
    {
        faabric::util::setMessageId(msg);
        REQUIRE(msg.timestamp() > baselineTimestamp);
    }
}

TEST_CASE(
  "Check message with ID already set still gets result key and status key",
  "[util]")
{
    faabric::Message msg;
    int msgId = 1234;
    msg.set_id(msgId);
    msg.set_statuskey("");
    msg.set_resultkey("");

    faabric::util::setMessageId(msg);
    REQUIRE(msg.statuskey() == faabric::util::statusKeyFromMessageId(msgId));
    REQUIRE(msg.resultkey() == faabric::util::resultKeyFromMessageId(msgId));
}

TEST_CASE("Test creating async response")
{
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");

    const std::string expected = std::to_string(msg.id());
    const std::string actual = faabric::util::buildAsyncResponse(msg);

    REQUIRE(expected == actual);
}
}
