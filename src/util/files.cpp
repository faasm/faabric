#include <faabric/util/bytes.h>
#include <faabric/util/files.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

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
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Couldn't open file " + path);
    }
    struct stat statbuf;
    int staterr = fstat(fd, &statbuf);
    if (staterr < 0) {
        throw std::runtime_error("Couldn't stat file " + path);
    }
    size_t fsize = statbuf.st_size;
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    std::vector<uint8_t> result;
    result.resize(fsize);
    int cpos = 0;
    while (cpos < fsize) {
        int rc = read(fd, result.data(), fsize - cpos);
        if (rc < 0) {
            perror("Couldn't read file");
            throw std::runtime_error("Couldn't read file " + path);
        } else {
            cpos += rc;
        }
    }
    close(fd);
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
