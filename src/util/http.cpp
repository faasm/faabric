#include "http.h"
#include "bytes.h"
#include "logging.h"

#include <pistache/async.h>
#include <pistache/client.h>
#include <pistache/http.h>
#include <pistache/http_header.h>
#include <pistache/net.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

using namespace Pistache;

namespace faabric::util {
std::vector<uint8_t> readFileFromUrl(const std::string& url)
{
    return readFileFromUrlWithHeader(url, nullptr);
}

std::vector<uint8_t> readFileFromUrlWithHeader(
  const std::string& url,
  const std::shared_ptr<Http::Header::Header>& header)
{
    Http::Client client;
    client.init();

    auto rb = client.get(url);

    if (header != nullptr) {
        rb.header(header);
    }

    // Set up the request and callbacks
    Async::Promise<Http::Response> resp =
      rb.timeout(std::chrono::milliseconds(HTTP_FILE_TIMEOUT)).send();

    std::stringstream out;
    Http::Code respCode;
    bool success = true;
    resp.then(
      [&](Http::Response response) {
          respCode = response.code();
          if (respCode == Http::Code::Ok) {
              auto body = response.body();
              out << body;
          } else {
              success = false;
          }
      },
      [&](std::exception_ptr exc) {
          PrintException excPrinter;
          excPrinter(exc);
          success = false;
      });

    // Make calls synchronous
    Async::Barrier<Http::Response> barrier(resp);
    std::chrono::milliseconds timeout(HTTP_FILE_TIMEOUT);
    barrier.wait_for(timeout);

    client.shutdown();

    // Check the response
    if (!success) {
        std::string msg =
          fmt::format("Error reading file from {} ({})", url, respCode);
        throw FileNotFoundAtUrlException(msg);
    } else if (out.str().empty()) {
        std::string msg = "Empty response for file " + url;
        throw FileNotFoundAtUrlException(msg);
    } else if (out.str() == IS_DIR_RESPONSE) {
        throw faabric::util::FileAtUrlIsDirectoryException(url +
                                                           " is a directory");
    }

    return faabric::util::stringToBytes(out.str());
}
}
