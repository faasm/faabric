#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/state/InMemoryStateKeyValue.h>
#include <faabric/state/State.h>
#include <faabric/state/StateClient.h>
#include <faabric/transport/common.h>
#include <faabric/util/bytes.h>
#include <faabric/util/config.h>
#include <faabric/util/macros.h>

#include <wait.h>

using namespace faabric::state;

namespace tests {

class SimpleStateServerTestFixture
  : public StateFixture
  , public ConfFixture
{
  public:
    SimpleStateServerTestFixture()
      : server(faabric::state::getGlobalState())
      , dataA({ 0, 1, 2, 3, 4, 5, 6, 7 })
      , dataB({ 7, 6, 5, 4, 3, 2, 1, 0 })
    {
        conf.stateMode = "inmemory";

        server.start();
    }

    ~SimpleStateServerTestFixture() { server.stop(); }

    std::shared_ptr<InMemoryStateKeyValue> getKv(const std::string& user,
                                                 const std::string& key,
                                                 size_t stateSize)
    {
        std::shared_ptr<StateKeyValue> localKv =
          state.getKV(user, key, stateSize);

        std::shared_ptr<InMemoryStateKeyValue> inMemLocalKv =
          std::static_pointer_cast<InMemoryStateKeyValue>(localKv);

        return inMemLocalKv;
    }

  protected:
    StateServer server;

    const char* userA = "foo";
    const char* keyA = "bar";
    const char* keyB = "baz";

    std::vector<uint8_t> dataA;
    std::vector<uint8_t> dataB;
};

TEST_CASE_METHOD(ConfFixture, "Test setting state server threads", "[state]")
{
    conf.stateServerThreads = 7;

    StateServer server(faabric::state::getGlobalState());

    REQUIRE(server.getNThreads() == 7);
}

TEST_CASE_METHOD(SimpleStateServerTestFixture,
                 "Test state request/ response",
                 "[state]")
{
    std::vector<uint8_t> actual(dataA.size(), 0);

    // Prepare a key-value with data
    auto kvA = getKv(userA, keyA, dataA.size());
    kvA->set(dataA.data());

    // Prepare a key-value with no data (but a size)
    auto kvB = getKv(userA, keyB, dataA.size());

    // Prepare a key-value with same key but different data (for pushing)
    std::string thisHost = faabric::util::getSystemConfig().endpointHost;
    auto kvADuplicate =
      InMemoryStateKeyValue(userA, keyA, dataB.size(), thisHost);
    kvADuplicate.set(dataB.data());

    StateClient client(userA, keyA, DEFAULT_STATE_HOST);

    SECTION("State size")
    {
        size_t actualSize = client.stateSize();
        REQUIRE(actualSize == dataA.size());
    }

    SECTION("State pull multi chunk")
    {
        std::vector<uint8_t> expectedA = { 1, 2, 3 };
        std::vector<uint8_t> expectedB = { 2, 3, 4, 5 };
        std::vector<uint8_t> expectedC = { 7 };

        // Deliberately overlap chunks
        StateChunk chunkA(1, 3, nullptr);
        StateChunk chunkB(2, 4, nullptr);
        StateChunk chunkC(7, 1, nullptr);

        std::vector<StateChunk> chunks = { chunkA, chunkB, chunkC };
        std::vector<uint8_t> expected = { 0, 1, 2, 3, 4, 5, 0, 7 };
        client.pullChunks(chunks, actual.data());
        REQUIRE(actual == expected);
    }

    SECTION("State push multi chunk")
    {
        std::vector<uint8_t> chunkDataA = { 7, 7 };
        std::vector<uint8_t> chunkDataB = { 8 };
        std::vector<uint8_t> chunkDataC = { 9, 9, 9 };

        // Deliberately overlap chunks
        StateChunk chunkA(0, chunkDataA);
        StateChunk chunkB(6, chunkDataB);
        StateChunk chunkC(1, chunkDataC);

        std::vector<StateChunk> chunks = { chunkA, chunkB, chunkC };
        client.pushChunks(chunks);

        // Check expectation
        std::vector<uint8_t> expected = { 7, 9, 9, 9, 4, 5, 8, 7 };
        kvA->get(actual.data());
        REQUIRE(actual == expected);
    }

    SECTION("State append")
    {
        // Append a few chunks
        std::vector<uint8_t> chunkA = { 3, 2, 1 };
        std::vector<uint8_t> chunkB = { 5, 5 };
        std::vector<uint8_t> chunkC = { 2, 2 };
        std::vector<uint8_t> expected = { 3, 2, 1, 5, 5, 2, 2 };

        client.append(chunkA.data(), chunkA.size());
        client.append(chunkB.data(), chunkB.size());
        client.append(chunkC.data(), chunkC.size());

        std::vector<uint8_t> actualAppended(expected.size(), 0);
        client.pullAppended(actualAppended.data(), actualAppended.size(), 3);

        REQUIRE(actualAppended == expected);
    }
}

TEST_CASE_METHOD(SimpleStateServerTestFixture,
                 "Test local-only push/ pull",
                 "[state]")
{
    // Create a key-value locally
    auto localKv = getKv(userA, keyA, dataA.size());

    // Check this host is the master
    REQUIRE(localKv->isMaster());

    // Set the data and push, check nothing breaks
    localKv->set(dataA.data());
    localKv->pushFull();

    // Check that we get the expected size
    State& state = getGlobalState();
    REQUIRE(state.getStateSize(userA, keyA) == dataA.size());
}

TEST_CASE_METHOD(SimpleStateServerTestFixture,
                 "Test local-only append",
                 "[state]")
{
    std::vector<uint8_t> chunkA = { 1, 1 };
    std::vector<uint8_t> chunkB = { 2, 2, 2 };
    std::vector<uint8_t> chunkC = { 3, 3 };
    std::vector<uint8_t> expected = { 1, 1, 2, 2, 2, 3, 3 };

    // Normal in-memory storage isn't used for append-only,
    // so size doesn't matter
    auto kv = getKv(userA, keyA, 1);

    kv->append(chunkA.data(), chunkA.size());
    kv->append(chunkB.data(), chunkB.size());
    kv->append(chunkC.data(), chunkC.size());

    size_t totalLength = chunkA.size() + chunkB.size() + chunkC.size();
    std::vector<uint8_t> actual(totalLength, 0);

    kv->getAppended(actual.data(), actual.size(), 3);

    REQUIRE(actual == expected);
}

TEST_CASE_METHOD(SimpleStateServerTestFixture,
                 "Test state server with local master",
                 "[state]")
{
    // Set and push
    auto localKv = getKv(userA, keyA, dataA.size());
    localKv->set(dataA.data());
    REQUIRE(localKv->isMaster());
    localKv->pushFull();

    // Modify locally
    localKv->set(dataB.data());

    // Pull
    const std::shared_ptr<StateKeyValue>& kv =
      state.getKV(userA, keyA, dataA.size());
    kv->pull();

    // Check it's still the same locally set value
    std::vector<uint8_t> actual(kv->get(), kv->get() + dataA.size());
    REQUIRE(actual == dataB);
}
}
