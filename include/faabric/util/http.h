#pragma once

#include "exception.h"
#include <pistache/http_header.h>
#include <string>
#include <vector>

#define FILE_PATH_HEADER "FilePath"
#define EMPTY_FILE_RESPONSE "Empty response"
#define HTTP_FILE_TIMEOUT 20000
#define IS_DIR_RESPONSE "IS_DIR"

using namespace Pistache;

namespace faabric::util {
std::vector<uint8_t> readFileFromUrl(const std::string& url);

std::vector<uint8_t> readFileFromUrlWithHeader(
  const std::string& url,
  const std::shared_ptr<Http::Header::Header>& header);

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
