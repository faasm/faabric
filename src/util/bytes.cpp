#include <faabric/util/bytes.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#include <sstream>
#include <stack>
#include <vector>

namespace faabric::util {

std::vector<uint8_t> stringToBytes(const std::string& str)
{
    if (str.empty()) {
        std::vector<uint8_t> empty;
        return empty;
    }

    // Get raw data as byte pointer
    const char* cstr = str.c_str();
    auto* rawBytes = reinterpret_cast<const uint8_t*>(cstr);

    // Wrap in bytes vector
    std::vector<uint8_t> actual(rawBytes, rawBytes + str.length());
    return actual;
}

void trimTrailingZeros(std::vector<uint8_t>& vectorIn)
{
    long i = vectorIn.size() - 1;

    while (i >= 0 && vectorIn.at((unsigned long)i) == 0) {
        i--;
    }

    if (i < 0) {
        vectorIn.clear();
    } else {
        vectorIn.resize((unsigned long)i + 1);
    }
}

int safeCopyToBuffer(const std::vector<uint8_t>& dataIn,
                     uint8_t* buffer,
                     int bufferLen)
{
    int dataSize = (int)dataIn.size();

    if (bufferLen <= 0) {
        return dataSize;
    }

    return safeCopyToBuffer(dataIn.data(), dataIn.size(), buffer, bufferLen);
}

int safeCopyToBuffer(const uint8_t* dataIn,
                     int dataLen,
                     uint8_t* buffer,
                     int bufferLen)
{
    if (dataLen == 0) {
        return 0;
    }

    // Truncate date being copied into a short buffer
    int copyLen = std::min(dataLen, bufferLen);
    std::copy(dataIn, dataIn + copyLen, buffer);

    return copyLen;
}

std::string bytesToString(const std::vector<uint8_t>& bytes)
{
    unsigned long byteLen = bytes.size();
    const char* charPtr = reinterpret_cast<const char*>(bytes.data());
    const std::string result = std::string(charPtr, charPtr + byteLen);

    return result;
}

std::string formatByteArrayToIntString(const std::vector<uint8_t>& bytes)
{
    std::stringstream ss;

    ss << "[";
    for (int i = 0; i < bytes.size(); i++) {
        ss << (int)bytes.at(i);

        if (i < bytes.size() - 1) {
            ss << ", ";
        }
    }
    ss << "]";

    return ss.str();
}

// This function is called in a tight loop over large regions of data so
// make sure it stays efficient.
std::vector<std::pair<uint32_t, uint32_t>> diffArrayRegions(
  std::span<const uint8_t> a,
  std::span<const uint8_t> b)
{
    PROF_START(ByteArrayDiff)
    std::vector<std::pair<uint32_t, uint32_t>> regions;

    // Iterate through diffs and work out start and finish offsets of each dirty
    // region
    uint32_t diffStart = 0;
    bool diffInProgress = false;
    for (uint32_t i = 0; i < a.size(); i++) {
        bool dirty = a.data()[i] != b.data()[i];
        if (dirty && !diffInProgress) {
            // Starts at this byte
            diffInProgress = true;
            diffStart = i;
        } else if (!dirty && diffInProgress) {
            // Finished on byte before
            diffInProgress = false;
            regions.emplace_back(diffStart, i - diffStart);
        }
    }

    // If we finish with a diff in progress, add it
    if (diffInProgress) {
        regions.emplace_back(diffStart, a.size() - diffStart);
    }

    PROF_END(ByteArrayDiff)
    return regions;
}

std::vector<bool> diffArrays(std::span<const uint8_t> a,
                             std::span<const uint8_t> b)
{
    if (a.size() != b.size()) {
        SPDLOG_ERROR(
          "Cannot diff arrays of different sizes {} != {}", a.size(), b.size());
        throw std::runtime_error("Cannot diff arrays of different sizes");
    }

    std::vector<bool> diffs(a.size(), false);
    for (int i = 0; i < a.size(); i++) {
        diffs[i] = a.data()[i] != b.data()[i];
    }

    return diffs;
}

}
