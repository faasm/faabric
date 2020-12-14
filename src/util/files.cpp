#include <faabric/util/files.h>
#include <faabric/util/bytes.h>
#include <faabric/util/logging.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

namespace faabric::util {
std::string readFileToString(const std::string& path)
{
    std::ifstream stream(path);
    std::stringstream buffer;
    buffer << stream.rdbuf();
    buffer.flush();

    return buffer.str();
}

std::vector<uint8_t> readFileToBytes(const std::string& path)
{
    std::ifstream file(path, std::ios::binary);

    // Stop eating new lines in binary mode
    file.unsetf(std::ios::skipws);

    // Reserve space
    std::streampos fileSize;
    file.seekg(0, std::ios::end);
    fileSize = file.tellg();

    std::vector<uint8_t> result;
    result.reserve(fileSize);

    // Read the data
    file.seekg(0, std::ios::beg);
    result.insert(result.begin(),
                  std::istreambuf_iterator<char>(file),
                  std::istreambuf_iterator<char>());

    return result;
}

void writeBytesToFile(const std::string& path, const std::vector<uint8_t>& data)
{
    std::ofstream outfile;
    outfile.open(path, std::ios::out | std::ios::binary);

    if (!outfile.is_open()) {
        throw std::runtime_error("Could not write to file " + path);
    }

    outfile.write((char*)data.data(), data.size());

    outfile.close();
}

size_t writeDataCallback(void* ptr, size_t size, size_t nmemb, void* stream)
{
    std::string data((const char*)ptr, (size_t)size * nmemb);
    *((std::stringstream*)stream) << data;
    return size * nmemb;
}

bool isWasm(const std::vector<uint8_t>& bytes)
{
    static const uint8_t wasmMagicNumber[4] = { 0x00, 0x61, 0x73, 0x6d };
    if (bytes.size() >= 4 && !memcmp(bytes.data(), wasmMagicNumber, 4)) {
        return true;
    } else {
        return false;
    }
}
}
