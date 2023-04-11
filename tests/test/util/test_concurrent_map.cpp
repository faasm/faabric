#include <iostream>

// Custom formatter must be defined before Catch2 is included
std::ostream& operator<<(std::ostream& os, std::pair<int, int> const& value)
{
    os << "(" << value.first << ", " << value.second << ")";
    return os;
}

#include <catch2/catch.hpp>

#include <faabric/util/concurrent_map.h>

#include <string>
#include <thread>
#include <utility>

using namespace faabric::util;
using Catch::Matchers::Equals;

namespace tests {

TEST_CASE("Test basic map operations in a single thread",
          "[util][concurrent_map]")
{
    const size_t initialCapacity = 32;
    ConcurrentMap<int, int> map(initialCapacity);

    REQUIRE(map.isEmpty());
    REQUIRE(map.size() == 0);
    REQUIRE(map.capacity() >= initialCapacity);
    REQUIRE(map.sortedKvPairs().empty());

    map.reserve(2 * initialCapacity);
    REQUIRE(map.capacity() >= 2 * initialCapacity);

    REQUIRE(map.insert(std::make_pair(1, 10)));
    REQUIRE(!map.insert(std::make_pair(1, 20))); // no-op
    REQUIRE(map.insert(std::make_pair(3, 30)));

    REQUIRE(!map.isEmpty());
    REQUIRE(map.size() == 2);
    REQUIRE_THAT(
      map.sortedKvPairs(),
      Equals(std::vector<std::pair<int, int>>{ { 1, 10 }, { 3, 30 } }));

    map.rehash(0);

    REQUIRE(!map.isEmpty());
    REQUIRE(map.size() == 2);
    REQUIRE_THAT(
      map.sortedKvPairs(),
      Equals(std::vector<std::pair<int, int>>{ { 1, 10 }, { 3, 30 } }));

    REQUIRE(!map.insertOrAssign(1, 20));
    REQUIRE(map.insertOrAssign(2, 20));

    REQUIRE(!map.isEmpty());
    REQUIRE(map.size() == 3);
    REQUIRE_THAT(map.sortedKvPairs(),
                 Equals(std::vector<std::pair<int, int>>{
                   { 1, 20 }, { 2, 20 }, { 3, 30 } }));

    REQUIRE(map.tryEmplace(4, 40));
    REQUIRE(map.tryEmplace(5, 50));
    REQUIRE(!map.tryEmplace(3, 50));

    REQUIRE(!map.isEmpty());
    REQUIRE(map.size() == 5);
    REQUIRE_THAT(map.sortedKvPairs(),
                 Equals(std::vector<std::pair<int, int>>{
                   { 1, 20 }, { 2, 20 }, { 3, 30 }, { 4, 40 }, { 5, 50 } }));

    map.erase(1);

    REQUIRE(!map.isEmpty());
    REQUIRE(map.size() == 4);
    REQUIRE_THAT(map.sortedKvPairs(),
                 Equals(std::vector<std::pair<int, int>>{
                   { 2, 20 }, { 3, 30 }, { 4, 40 }, { 5, 50 } }));

    int called = 0;
    map.tryEmplaceThenMutate(
      1,
      [&](bool placed, int& val) {
          called++;
          REQUIRE(placed);
          REQUIRE(val == 0);
          val = 10;
      },
      0);
    REQUIRE(called == 1);

    REQUIRE(!map.isEmpty());
    REQUIRE(map.size() == 5);
    REQUIRE_THAT(map.sortedKvPairs(),
                 Equals(std::vector<std::pair<int, int>>{
                   { 1, 10 }, { 2, 20 }, { 3, 30 }, { 4, 40 }, { 5, 50 } }));

    called = 0;
    REQUIRE(map.mutate(5, [&](int& value) {
        called++;
        REQUIRE(value == 50);
        value = 51;
    }));
    REQUIRE(called == 1);

    called = 0;
    REQUIRE(!map.mutate(7, [&](int& value) {
        called++;
        value = 70;
    }));
    REQUIRE(called == 0);

    called = 0;
    REQUIRE(map.inspect(4, [&](const int& value) {
        called++;
        REQUIRE(value == 40);
    }));
    REQUIRE(called == 1);

    called = 0;
    REQUIRE(map.inspect(4, [&](int value) {
        called++;
        REQUIRE(value == 40);
    }));
    REQUIRE(called == 1);

    called = 0;
    REQUIRE(!map.inspect(7, [&](const int& value) { called++; }));
    REQUIRE(called == 0);

    REQUIRE(map.get(7) == std::nullopt);
    REQUIRE(!map.contains(7));
    REQUIRE(map.get(4) == 40);
    REQUIRE(map.contains(4));

    REQUIRE_THAT(map.sortedKvPairs(),
                 Equals(std::vector<std::pair<int, int>>{
                   { 1, 10 }, { 2, 20 }, { 3, 30 }, { 4, 40 }, { 5, 51 } }));

    called = 0;
    int sum = 0;
    map.inspectAll([&](const int& key, const int& value) {
        called++;
        sum += value;
    });
    REQUIRE(sum == (10 + 20 + 30 + 40 + 51));
    REQUIRE(called == 5);

    called = 0;
    map.mutateAll([&](const int& key, int& value) {
        called++;
        value /= 10;
    });
    REQUIRE(called == 5);
    REQUIRE(map.size() == 5);
    REQUIRE_THAT(map.sortedKvPairs(),
                 Equals(std::vector<std::pair<int, int>>{
                   { 1, 1 }, { 2, 2 }, { 3, 3 }, { 4, 4 }, { 5, 5 } }));

    map.eraseIf([](const int& key, const int& value) { return key <= 3; });
    REQUIRE(map.size() == 2);
    REQUIRE_THAT(
      map.sortedKvPairs(),
      Equals(std::vector<std::pair<int, int>>{ { 4, 4 }, { 5, 5 } }));

    map.clear();
    REQUIRE(map.isEmpty());
    REQUIRE(map.size() == 0);
    REQUIRE(map.sortedKvPairs().empty());
}

TEST_CASE("Test insertion from many threads", "[util][concurrent_map]")
{
    const int nThreads = 5;
    const int nValues = 1000;
    // undersized to make sure it resizes at least once
    ConcurrentMap<int, int> map(nValues);

    REQUIRE(map.isEmpty());
    std::vector<std::jthread> workers;
    for (int thread = 0; thread < nThreads; thread++) {
        workers.emplace_back([thread, &map]() {
            for (int i = 0; i < nValues; i++) {
                map.tryEmplace(nThreads * i + thread, i);
            }
        });
    }
    for (std::jthread& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers.clear();

    auto values = map.sortedKvPairs();
    for (int i = 0; i < nValues; i++) {
        for (int thread = 0; thread < nThreads; thread++) {
            int key = nThreads * i + thread;
            REQUIRE(values.at(key).first == (key));
            REQUIRE(values.at(key).second == i);
        }
    }
}

}
