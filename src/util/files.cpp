#include "files.h"
#include "bytes.h"
#include "logging.h"
#include "pistache/async.h"
#include "pistache/http_header.h"

#include <pistache/client.h>
#include <pistache/http.h>
#include <pistache/net.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#define HTTP_FILE_TIMEOUT 20000

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

std::vector<uint8_t> readFileFromUrl(const std::string& url)
{
    return readFileFromUrlWithHeader(url, nullptr);
}

std::vector<uint8_t> readFileFromUrlWithHeader(
  const std::string& url,
  const std::shared_ptr<Http::Header::Header>& header)
{
    auto logger = faabric::util::getLogger();

    Http::Client client;
    client.init();

    auto rb = client.get(url);

    if (header != nullptr) {
        rb.header(header);
    }

    Async::Promise<Http::Response> resp = rb.send();

    std::stringstream out;
    resp.then(
      [&](Http::Response response) {
          if (response.code() == Http::Code::Ok) {
              auto body = response.body();
              out << body;
          } else {
              logger->error(
                "Failed request to {}, code = {}", url, response.code());
          }
      },
      [&](std::exception_ptr exc) {
          PrintException excPrinter;
          excPrinter(exc);
      });

    Async::Barrier<Http::Response> barrier(resp);
    std::chrono::milliseconds timeout(HTTP_FILE_TIMEOUT);
    barrier.wait_for(timeout);

    client.shutdown();

    return faabric::util::stringToBytes(out.str());
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
