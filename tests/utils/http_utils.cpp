#include <faabric/util/bytes.h>
#include <faabric/util/http.h>
#include <faabric/util/logging.h>

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

namespace tests {

std::pair<int, std::string> getRequestToUrl(std::string host,
                                            int port,
                                            std::string url)
{
    Http::Client client;
    client.init();

    std::string fullUrl = fmt::format("{}:{}/{}", host, port, url);
    auto rb = client.get(fullUrl);

    // Set up the request and callbacks
    Async::Promise<Http::Response> resp =
      rb.timeout(std::chrono::milliseconds(HTTP_FILE_TIMEOUT)).send();

    std::stringstream out;
    Http::Code respCode;
    resp.then(
      [&](Http::Response response) {
          respCode = response.code();
          if (respCode == Http::Code::Ok) {
              auto body = response.body();
              out << body;
          }
      },
      [&](std::exception_ptr exc) {
          PrintException excPrinter;
          excPrinter(exc);
      });

    // Make calls synchronous
    Async::Barrier<Http::Response> barrier(resp);
    std::chrono::milliseconds timeout(HTTP_FILE_TIMEOUT);
    barrier.wait_for(timeout);

    client.shutdown();

    return std::make_pair((int)respCode, out.str());
}
}
