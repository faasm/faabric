#pragma once

#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>

#include <faabric/util/macros.h>

namespace faabric::util {
std::vector<uint8_t> stringToBytes(const std::string& str);

std::string bytesToString(const std::vector<uint8_t>& bytes);

std::string formatByteArrayToIntString(const std::vector<uint8_t>& bytes);

void trimTrailingZeros(std::vector<uint8_t>& vectorIn);

int safeCopyToBuffer(const std::vector<uint8_t>& dataIn,
                     uint8_t* buffer,
                     int bufferLen);

int safeCopyToBuffer(const uint8_t* dataIn,
                     int dataLen,
                     uint8_t* buffer,
                     int bufferLen);

template<class T>
T unalignedRead(const uint8_t *bytes)
{
    T value;
    std::copy_n(bytes, sizeof(T), reinterpret_cast<uint8_t*>(&value));
    return value;
}

template<class T>
void unalignedWrite(const T& value, uint8_t *destination)
{
    std::copy_n(
      reinterpret_cast<const uint8_t*>(&value), sizeof(T), destination);
}

template<class T>
void appendBytesOf(std::vector<uint8_t>& container, T value)
{
    uint8_t* start = reinterpret_cast<uint8_t*>(&value);
    uint8_t* end = reinterpret_cast<uint8_t*>(&value) + sizeof(T);
    container.insert(container.end(), start, end);
}

template<class T>
size_t readBytesOf(const std::vector<uint8_t>& container,
                   size_t offset,
                   T* outValue)
{
    if (offset >= container.size() || offset + sizeof(T) > container.size()) {
        throw std::range_error("Trying to read bytes out of container range");
    }
    // use byte pointers to make sure there are no alignment issues
    uint8_t* outStart = reinterpret_cast<uint8_t*>(outValue);
    std::copy_n(container.data() + offset, sizeof(T), outStart);
    return offset + sizeof(T);
}

template<typename T>
std::vector<uint8_t> valueToBytes(T val)
{
    return std::vector(BYTES(&val), BYTES(&val) + sizeof(T));
}
}
