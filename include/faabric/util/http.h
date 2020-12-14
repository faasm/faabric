#pragma once

#include <faabric/util/exception.h>
#include <pistache/http_header.h>
#include <string>
#include <vector>

#define HTTP_FILE_TIMEOUT 20000

using namespace Pistache;

namespace faabric::util {
std::vector<uint8_t> readFileFromUrl(const std::string& url);

std::vector<uint8_t> readFileFromUrlWithHeader(
  const std::string& url,
  const std::shared_ptr<Http::Header::Header>& header);

class FaabricHttpException : public faabric::util::FaabricException
{
  public:
    explicit FaabricHttpException(std::string message)
      : FaabricException(std::move(message))
    {}
};
}
