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

std::string byteArrayToHexString(const uint8_t* data, int dataSize)
{
    std::stringstream ss;
    ss << std::hex;

    for (int i = 0; i < dataSize; ++i) {
        ss << std::setw(2) << std::setfill('0') << static_cast<int>(data[i]);
    }

    return ss.str();
}

std::vector<uint8_t> hexStringToByteArray(const std::string& hexString)
{
    if (hexString.length() % 2 != 0) {
        SPDLOG_ERROR("The provided hex string has not an even number of"
                     "characters. Can't convert {} to a byte array",
                     hexString);
        throw std::runtime_error("Provided hex string has not even length");
    }

    std::vector<uint8_t> byteArray;
    byteArray.reserve(hexString.length() / 2);

    for (int i = 0; i < hexString.length(); i += 2) {
        std::string byteString = hexString.substr(i, 2);
        uint8_t byteChar = (uint8_t)strtol(byteString.c_str(), nullptr, 16);
        byteArray.push_back(byteChar);
    }

    return byteArray;
}
}
