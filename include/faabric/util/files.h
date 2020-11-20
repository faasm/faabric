#pragma once

#include "exception.h"
#include <string>
#include <vector>

#define FILE_PATH_HEADER "FilePath"
#define EMPTY_FILE_RESPONSE "Empty response"

namespace faabric::util {
std::string readFileToString(const std::string& path);

std::vector<uint8_t> readFileToBytes(const std::string& path);

void writeBytesToFile(const std::string& path,
                      const std::vector<uint8_t>& data);

std::vector<uint8_t> readFileFromUrl(const std::string& url);

std::vector<uint8_t> readFileFromUrlWithHeader(const std::string& url,
                                               const std::string& header);

bool isWasm(const std::vector<uint8_t>& bytes);

class FileNotFoundAtUrlException : public faabric::util::FaabricException
{
  public:
    explicit FileNotFoundAtUrlException(std::string message)
      : FaabricException(std::move(message))
    {}
};

class FileAtUrlIsDirectoryException : public faabric::util::FaabricException
{
  public:
    explicit FileAtUrlIsDirectoryException(std::string message)
      : FaabricException(std::move(message))
    {}
};
}
