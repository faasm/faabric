#include <catch2/catch.hpp>

#include <algorithm>
#include <faabric/util/delta.h>

using namespace faabric::util;

namespace tests {

TEST_CASE("Test parsing DeltaSettings from configuration strings",
          "[util][delta]")
{
    SECTION("Empty")
    {
        DeltaSettings empty("");
        REQUIRE(empty.usePages == false);
        REQUIRE(empty.xorWithOld == false);
        REQUIRE(empty.useZstd == false);
    }

    SECTION("Just pages")
    {
        DeltaSettings justPages("pages=64;");
        REQUIRE(justPages.usePages == true);
        REQUIRE(justPages.pageSize == 64);
        REQUIRE(justPages.xorWithOld == false);
        REQUIRE(justPages.useZstd == false);
    }

    SECTION("Just pages 2")
    {
        DeltaSettings justPages2("pages=64");
        REQUIRE(justPages2.usePages == true);
        REQUIRE(justPages2.pageSize == 64);
        REQUIRE(justPages2.xorWithOld == false);
        REQUIRE(justPages2.useZstd == false);
    }

    SECTION("Just xor")
    {
        DeltaSettings justXor("xor;");
        REQUIRE(justXor.usePages == false);
        REQUIRE(justXor.xorWithOld == true);
        REQUIRE(justXor.useZstd == false);
    }

    SECTION("Just xor 2")
    {
        DeltaSettings justXor2("xor");
        REQUIRE(justXor2.usePages == false);
        REQUIRE(justXor2.xorWithOld == true);
        REQUIRE(justXor2.useZstd == false);
    }

    SECTION("Just zstd")
    {
        DeltaSettings justZstd("zstd=-3;");
        REQUIRE(justZstd.usePages == false);
        REQUIRE(justZstd.xorWithOld == false);
        REQUIRE(justZstd.useZstd == true);
        REQUIRE(justZstd.zstdLevel == -3);
    }

    SECTION("Just zstd 2")
    {
        DeltaSettings justZstd2("zstd=7");
        REQUIRE(justZstd2.usePages == false);
        REQUIRE(justZstd2.xorWithOld == false);
        REQUIRE(justZstd2.useZstd == true);
        REQUIRE(justZstd2.zstdLevel == 7);
    }

    SECTION("All options")
    {
        DeltaSettings allOptions("pages=128;xor;zstd=7;");
        REQUIRE(allOptions.usePages == true);
        REQUIRE(allOptions.pageSize == 128);
        REQUIRE(allOptions.xorWithOld == true);
        REQUIRE(allOptions.useZstd == true);
        REQUIRE(allOptions.zstdLevel == 7);
    }

    SECTION("All options 2")
    {
        DeltaSettings allOptions2("pages=128;xor;zstd=7");
        REQUIRE(allOptions2.usePages == true);
        REQUIRE(allOptions2.pageSize == 128);
        REQUIRE(allOptions2.xorWithOld == true);
        REQUIRE(allOptions2.useZstd == true);
        REQUIRE(allOptions2.zstdLevel == 7);
    }
}

TEST_CASE("Test delta calculate and apply", "[util][delta]")
{
    std::array<DeltaSettings, 13> settingsVariants{ {
      DeltaSettings(""),
      DeltaSettings("pages=4096"),
      DeltaSettings("xor"),
      DeltaSettings("zstd=3"),
      DeltaSettings("zstd=-7"),
      DeltaSettings("pages=4096;xor"),
      DeltaSettings("xor;zstd=3"),
      DeltaSettings("xor;zstd=-7"),
      DeltaSettings("pages=4096;zstd=3"),
      DeltaSettings("pages=4096;zstd=3"),
      DeltaSettings("pages=4096;xor;zstd=3"),
      DeltaSettings("pages=4096;xor;zstd=1"),
      DeltaSettings("pages=4096;xor;zstd=-7"),
    } };
    std::vector<uint8_t> oldMem(65536, 0);
    std::fill(oldMem.begin() + 10000, oldMem.begin() + 20000, 17);
    std::fill(oldMem.begin() + 20000, oldMem.begin() + 30000, 37);
    std::vector<uint8_t> newMem(oldMem);
    newMem.resize(131072);
    std::fill(newMem.begin() + 15000, newMem.begin() + 19000, 12);
    std::fill(newMem.begin() + 29000, newMem.begin() + 32000, 99);
    std::fill(newMem.begin() + 100000, newMem.begin() + 129000, 127);
    for (const auto& cfg : settingsVariants) {
        SECTION(cfg.toString())
        {
            auto delta = serializeDelta(
              cfg, oldMem.data(), oldMem.size(), newMem.data(), newMem.size());
            REQUIRE(delta.size() > 2);
            std::vector<uint8_t> appliedMem(oldMem);
            REQUIRE(std::equal(oldMem.cbegin(),
                               oldMem.cend(),
                               appliedMem.cbegin(),
                               appliedMem.cend()));
            applyDelta(
              delta,
              [&appliedMem](uint32_t newSize) { appliedMem.resize(newSize); },
              [&appliedMem]() { return appliedMem.data(); });
            REQUIRE(std::equal(newMem.cbegin(),
                               newMem.cend(),
                               appliedMem.cbegin(),
                               appliedMem.cend()));
        }
    }
}

}
