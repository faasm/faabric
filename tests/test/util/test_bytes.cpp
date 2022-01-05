#include <catch2/catch.hpp>
#include <faabric/util/bytes.h>

using namespace faabric::util;

namespace tests {

TEST_CASE("Test converting integers to bytes", "[util]")
{
    std::string input = "abcde";

    std::vector<uint8_t> actual = stringToBytes(input);

    // Check cast back to char
    REQUIRE('a' == (char)actual[0]);
    REQUIRE('b' == (char)actual[1]);
    REQUIRE('c' == (char)actual[2]);
    REQUIRE('d' == (char)actual[3]);
}

TEST_CASE("Test removing trailing zeros", "[util]")
{
    std::vector<uint8_t> input = { 0, 2, 10, 0, 32, 0, 0, 0, 0 };

    trimTrailingZeros(input);

    REQUIRE(input.size() == 5);
    std::vector<uint8_t> expected = { 0, 2, 10, 0, 32 };
    REQUIRE(input == expected);
}

TEST_CASE("Test removing trailing zeros on all zeros", "[util]")
{
    std::vector<uint8_t> input = { 0, 0, 0, 0, 0 };

    trimTrailingZeros(input);

    REQUIRE(input.empty());
}

TEST_CASE("Test safe copy to small buffer", "[util]")
{
    std::vector<uint8_t> buffer = { 0, 1, 2, 3, 4, 5, 6 };

    uint8_t smallBuff[3];
    safeCopyToBuffer(buffer, smallBuff, 3);

    REQUIRE(smallBuff[0] == 0);
    REQUIRE(smallBuff[1] == 1);
    REQUIRE(smallBuff[2] == 2);
}

TEST_CASE("Test safe copy to big buffer", "[util]")
{
    std::vector<uint8_t> buffer = { 0, 1, 2, 3, 4, 5, 6 };

    uint8_t bigBuff[20];
    safeCopyToBuffer(buffer, bigBuff, 20);

    std::vector<uint8_t> actual(bigBuff, bigBuff + 7);
    REQUIRE(actual == buffer);
}

TEST_CASE("Test safe copy empty data ignored", "[util]")
{
    std::vector<uint8_t> buffer;

    uint8_t recvBuf[3] = { 0, 0, 0 };
    int res = safeCopyToBuffer(buffer, recvBuf, 3);
    REQUIRE(res == 0);
    REQUIRE(recvBuf[0] == 0);
    REQUIRE(recvBuf[1] == 0);
    REQUIRE(recvBuf[2] == 0);
}

TEST_CASE("Test safe copy with long buffer and other chars", "[util]")
{
    std::string input = "abc/def.com";
    const std::vector<uint8_t> inputBytes = stringToBytes(input);

    uint8_t buffer[20];
    safeCopyToBuffer(inputBytes, buffer, 20);

    // Add null-terminator
    buffer[11] = '\0';

    // Check cast back to char
    REQUIRE('a' == (char)buffer[0]);
    REQUIRE('b' == (char)buffer[1]);
    REQUIRE('c' == (char)buffer[2]);
    REQUIRE('/' == (char)buffer[3]);
    REQUIRE('d' == (char)buffer[4]);
    REQUIRE('e' == (char)buffer[5]);
    REQUIRE('f' == (char)buffer[6]);
    REQUIRE('.' == (char)buffer[7]);
    REQUIRE('c' == (char)buffer[8]);
    REQUIRE('o' == (char)buffer[9]);
    REQUIRE('m' == (char)buffer[10]);
    REQUIRE('\0' == (char)buffer[11]);

    // Check conversion back to string
    char* actualStr = reinterpret_cast<char*>(buffer);
    REQUIRE(input == actualStr);
}

TEST_CASE("Test integer encoding to/from bytes", "[util]")
{
    std::vector<uint8_t> buffer;

    uint8_t v1 = 2, r1;
    uint16_t v2 = 0xABCD, r2;
    uint32_t v4 = 0xBEEF1337, r4;
    uint64_t v8 = 0xABEE2929BEE51234, r8;

    appendBytesOf(buffer, v1);
    REQUIRE(buffer.size() == 1);
    appendBytesOf(buffer, v2);
    REQUIRE(buffer.size() == (1 + 2));
    appendBytesOf(buffer, v4);
    REQUIRE(buffer.size() == (1 + 2 + 4));
    appendBytesOf(buffer, v8);
    REQUIRE(buffer.size() == (1 + 2 + 4 + 8));

    size_t offset = 0;
    offset = readBytesOf(buffer, offset, &r1);
    REQUIRE(r1 == v1);
    REQUIRE(offset == 1);
    offset = readBytesOf(buffer, offset, &r2);
    REQUIRE(r2 == v2);
    REQUIRE(offset == (1 + 2));
    offset = readBytesOf(buffer, offset, &r4);
    REQUIRE(r4 == v4);
    REQUIRE(offset == (1 + 2 + 4));
    offset = readBytesOf(buffer, offset, &r8);
    REQUIRE(r8 == v8);
    REQUIRE(offset == (1 + 2 + 4 + 8));
    REQUIRE_THROWS_AS(readBytesOf(buffer, offset, &r1), std::range_error);
}

TEST_CASE("Test format byte array to string", "[util]")
{
    std::vector<uint8_t> bytesIn;
    std::string expectedString;

    SECTION("Empty") { expectedString = "[]"; }

    SECTION("Non-empty")
    {
        bytesIn = { 0, 1, 2, 3, 4, 5, 6, 7 };
        expectedString = "[0, 1, 2, 3, 4, 5, 6, 7]";
    }

    SECTION("Larger int values")
    {
        bytesIn = { 23, 9, 100 };
        expectedString = "[23, 9, 100]";
    }

    REQUIRE(formatByteArrayToIntString(bytesIn) == expectedString);
}

TEST_CASE("Test diffing byte array regions", "[util]")
{
    std::vector<uint8_t> a;
    std::vector<uint8_t> b;
    std::vector<std::pair<uint32_t, uint32_t>> expected;

    SECTION("Equal")
    {
        a = { 0, 1, 2, 3 };
        b = { 0, 1, 2, 3 };
    }

    SECTION("Empty") {}

    SECTION("Not equal")
    {
        a = { 0, 0, 2, 2, 3, 3, 4, 4, 5, 5 };
        b = { 0, 1, 1, 2, 3, 6, 6, 6, 5, 5 };
        expected = {
            { 1, 2 },
            { 5, 3 },
        };
    }

    SECTION("Single length")
    {
        a = {0, 1, 2, 3, 4};
        b = {0, 1, 3, 3, 4};
        expected = {
            {2, 1}
        };
    }

    SECTION("Difference at start")
    {
        a = {0, 1, 2, 3, 4, 5, 6};
        b = {1, 2, 3, 3, 3, 4, 6};
        expected = {
            {0, 3},
            {4, 2}
        };
    }

    SECTION("Difference at end")
    {
        a = {0, 1, 2, 3, 4, 5, 6};
        b = {0, 1, 1, 3, 3, 4, 5};
        expected = {
            {2, 1},
            {4, 3}
        };
    }

    std::vector<std::pair<uint32_t, uint32_t>> actual =
      diffArrayRegions({ a.data(), a.size() }, { b.data(), b.size() });

    REQUIRE(actual.size() == expected.size());
    for (int i = 0; i < actual.size(); i++) {
        REQUIRE(actual.at(i).first == expected.at(i).first);
        REQUIRE(actual.at(i).second == expected.at(i).second);
    }
}

TEST_CASE("Test diffing byte arrays", "[util]")
{
    std::vector<uint8_t> a;
    std::vector<uint8_t> b;
    std::vector<bool> expected;

    SECTION("Equal")
    {
        a = { 0, 1, 2, 3 };
        b = { 0, 1, 2, 3 };
        expected = std::vector<bool>(a.size(), false);
    }

    SECTION("Empty") {}

    SECTION("Not equal")
    {
        a = { 0, 0, 2, 2, 3, 3, 4, 4, 5, 5 };
        b = { 0, 1, 1, 2, 3, 6, 6, 6, 5, 5 };
        expected = { false, true, true, false, false,
                     true,  true, true, false, false };
    }

    std::vector<bool> actual =
      diffArrays({ a.data(), a.size() }, { b.data(), b.size() });

    REQUIRE(actual == expected);
}
}
