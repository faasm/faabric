#include <faabric/util/bytes.h>
#include <faabric/util/delta.h>

#include <zstd.h>

#include <algorithm>
#include <cassert>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string_view>

namespace faabric::util {

DeltaSettings::DeltaSettings(const std::string& definition)
  : usePages(false)
  , pageSize(4096)
  , xorWithOld(false)
  , useZstd(false)
  , zstdLevel(1)
{
    std::stringstream ss(definition);
    std::string part;
    while (std::getline(ss, part, ';')) {
        if (part.size() < 1) {
            continue;
        }
        if (std::string_view pfx = "pages="; part.find(pfx.data(), 0) == 0) {
            this->usePages = true;
            this->pageSize = std::stoul(part.substr(pfx.size()));
        } else if (part == "xor") {
            this->xorWithOld = true;
        } else if (std::string_view pfx = "zstd=";
                   part.find(pfx.data(), 0) == 0) {
            this->useZstd = true;
            this->zstdLevel = std::stoi(part.substr(pfx.size()));
        } else {
            throw std::invalid_argument(
              std::string("Invalid DeltaSettings configuration argument: ") +
              part);
        }
    }
}

std::string DeltaSettings::toString() const
{
    std::stringstream ss;
    if (this->usePages) {
        ss << "pages=" << this->pageSize << ';';
    }
    if (this->xorWithOld) {
        ss << "xor;";
    }
    if (this->useZstd) {
        ss << "zstd=" << this->zstdLevel << ';';
    }
    return ss.str();
}

std::vector<uint8_t> serializeDelta(const DeltaSettings& cfg,
                                    const uint8_t* oldDataStart,
                                    size_t oldDataLen,
                                    const uint8_t* newDataStart,
                                    size_t newDataLen)
{
    std::vector<uint8_t> outb;
    outb.reserve(16384);
    outb.push_back(DELTA_PROTOCOL_VERSION);
    outb.push_back(DELTACMD_TOTAL_SIZE);
    appendBytesOf(outb, uint32_t(newDataLen));
    auto encodeChangedRegion = [&](size_t startByte, size_t newLength) {
        if (newLength == 0) {
            return;
        }
        assert(startByte <= newDataLen);
        size_t endByte = startByte + newLength;
        assert(endByte <= newDataLen);
        assert(startByte <= oldDataLen);
        assert(endByte <= oldDataLen);
        if (cfg.xorWithOld) {
            outb.push_back(DELTACMD_DELTA_XOR);
            appendBytesOf(outb, uint32_t(startByte));
            appendBytesOf(outb, uint32_t(newLength));
            size_t xorStart = outb.size();
            outb.insert(
              outb.end(), newDataStart + startByte, newDataStart + endByte);
            auto xorBegin = outb.begin() + xorStart;
            std::transform(oldDataStart + startByte,
                           oldDataStart + endByte,
                           xorBegin,
                           xorBegin,
                           std::bit_xor<uint8_t>());
        } else {
            outb.push_back(DELTACMD_DELTA_OVERWRITE);
            appendBytesOf(outb, uint32_t(startByte));
            appendBytesOf(outb, uint32_t(newLength));
            outb.insert(
              outb.end(), newDataStart + startByte, newDataStart + endByte);
        }
    };
    auto encodeNewRegion = [&](size_t newStart, size_t newLength) {
        if (newLength == 0) {
            return;
        }
        assert(newStart <= newDataLen);
        size_t newEnd = newStart + newLength;
        assert(newEnd <= newDataLen);
        outb.push_back(DELTACMD_DELTA_OVERWRITE);
        appendBytesOf(outb, uint32_t(newStart));
        appendBytesOf(outb, uint32_t(newLength));
        outb.insert(outb.end(), newDataStart + newStart, newDataStart + newEnd);
    };
    if (cfg.usePages) {
        for (size_t pageStart = 0; pageStart < newDataLen;
             pageStart += cfg.pageSize) {
            size_t pageEnd = pageStart + cfg.pageSize;
            bool startInBoth = (pageStart < oldDataLen);
            bool endInBoth = (pageEnd <= newDataLen) && (pageEnd <= oldDataLen);
            if (startInBoth && endInBoth) {
                bool anyChanges = !std::equal(newDataStart + pageStart,
                                              newDataStart + pageEnd,
                                              oldDataStart);
                if (anyChanges) {
                    encodeChangedRegion(pageStart, cfg.pageSize);
                }
            } else if (!startInBoth) {
                using namespace std::placeholders;
                if (std::any_of(
                      newDataStart + pageStart,
                      newDataStart + pageEnd,
                      std::bind(std::not_equal_to<uint8_t>(), 0, _1))) {
                    encodeNewRegion(pageStart,
                                    std::min(pageEnd, newDataLen) - pageStart);
                }
            } else {
                encodeNewRegion(pageStart,
                                std::min(pageEnd, newDataLen) - pageStart);
            }
        }
    } else {
        if (newDataLen >= oldDataLen) {
            encodeChangedRegion(0, oldDataLen);
            encodeNewRegion(oldDataLen, newDataLen - oldDataLen);
        } else {
            encodeChangedRegion(0, newDataLen);
            // discard longer old data
        }
    }
    outb.push_back(DELTACMD_END);
    outb.shrink_to_fit();
    if (!cfg.useZstd) {
        return outb;
    } else {
        std::vector<uint8_t> compressBuffer;
        size_t compressBound = ZSTD_compressBound(outb.size());
        compressBuffer.reserve(compressBound + 19);
        compressBuffer.push_back(DELTA_PROTOCOL_VERSION);
        compressBuffer.push_back(DELTACMD_ZSTD_COMPRESSED_COMMANDS);
        size_t idxComprLen = compressBuffer.size();
        appendBytesOf(compressBuffer, uint64_t(0xDEAD)); // to be filled in
        appendBytesOf(compressBuffer, uint64_t(outb.size()));
        size_t idxCDataStart = compressBuffer.size();
        compressBuffer.insert(compressBuffer.end(), compressBound, uint8_t(0));
        auto zstdResult = ZSTD_compress(compressBuffer.data() + idxCDataStart,
                                        compressBuffer.size() - idxCDataStart,
                                        outb.data(),
                                        outb.size(),
                                        cfg.zstdLevel);
        if (ZSTD_isError(zstdResult)) {
            auto error = ZSTD_getErrorName(zstdResult);
            throw std::runtime_error(std::string("ZSTD compression error: ") +
                                     error);
        } else {
            compressBuffer.resize(idxCDataStart + zstdResult);
        }
        {
            uint64_t comprLen = zstdResult;
            std::copy_n(reinterpret_cast<uint8_t*>(&comprLen),
                        sizeof(uint64_t),
                        compressBuffer.data() + idxComprLen);
        }
        compressBuffer.push_back(DELTACMD_END);
        compressBuffer.shrink_to_fit();
        return compressBuffer;
    }
}

void applyDelta(const std::vector<uint8_t>& delta,
                std::function<void(uint32_t)> setDataSize,
                std::function<uint8_t*()> getDataPointer)
{
    size_t deltaLen = delta.size();
    if (deltaLen < 2) {
        throw std::runtime_error("Delta too short to be valid");
    }
    if (delta.at(0) != DELTA_PROTOCOL_VERSION) {
        throw std::runtime_error("Unsupported delta version");
    }
    size_t readIdx = 1;
    while (readIdx < deltaLen) {
        uint8_t cmd = delta.at(readIdx);
        readIdx++;
        switch (cmd) {
            case DELTACMD_TOTAL_SIZE: {
                uint32_t totalSize{};
                readIdx = readBytesOf(delta, readIdx, &totalSize);
                setDataSize(totalSize);
                break;
            }
            case DELTACMD_ZSTD_COMPRESSED_COMMANDS: {
                uint64_t compressedSize{}, decompressedSize{};
                readIdx = readBytesOf(delta, readIdx, &compressedSize);
                readIdx = readBytesOf(delta, readIdx, &decompressedSize);
                if (readIdx + compressedSize > deltaLen) {
                    throw std::range_error(
                      "Delta compressed commands block goes out of range:");
                }
                std::vector<uint8_t> decompressedCmds(decompressedSize, 0);
                auto zstdResult = ZSTD_decompress(decompressedCmds.data(),
                                                  decompressedCmds.size(),
                                                  delta.data() + readIdx,
                                                  compressedSize);
                if (ZSTD_isError(zstdResult)) {
                    auto error = ZSTD_getErrorName(zstdResult);
                    throw std::runtime_error(
                      std::string("ZSTD compression error: ") + error);
                } else if (zstdResult != decompressedCmds.size()) {
                    throw std::runtime_error(
                      "Mismatched decompression sizes in the NDP delta");
                }
                applyDelta(decompressedCmds, setDataSize, getDataPointer);
                readIdx += compressedSize;
                break;
            }
            case DELTACMD_DELTA_OVERWRITE: {
                uint32_t offset{}, length{};
                readIdx = readBytesOf(delta, readIdx, &offset);
                readIdx = readBytesOf(delta, readIdx, &length);
                if (readIdx + length > deltaLen) {
                    throw std::range_error(
                      "Delta overwrite block goes out of range");
                }
                uint8_t* data = getDataPointer();
                std::copy_n(delta.data() + readIdx, length, data + offset);
                readIdx += length;
                break;
            }
            case DELTACMD_DELTA_XOR: {
                uint32_t offset{}, length{};
                readIdx = readBytesOf(delta, readIdx, &offset);
                readIdx = readBytesOf(delta, readIdx, &length);
                if (readIdx + length > deltaLen) {
                    throw std::range_error("Delta XOR block goes out of range");
                }
                uint8_t* data = getDataPointer();
                std::transform(delta.data() + readIdx,
                               delta.data() + readIdx + length,
                               data + offset,
                               data + offset,
                               std::bit_xor<uint8_t>());
                readIdx += length;
                break;
            }
            case DELTACMD_END: {
                readIdx = deltaLen;
                break;
            }
        }
    }
}

}
