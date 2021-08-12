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

#define HTTP_REQ_TIMEOUT 5000

namespace tests {

std::pair<int, std::string> submitGetRequestToUrl(std::string host,
                                                  int port)

{
    Http::Client client;
    client.init();

    std::string fullUrl = fmt::format("{}:{}", host, port);
    SPDLOG_DEBUG("Making HTTP GET request to {}", fullUrl);

    // Set up the request and callbacks
    Async::Promise<Http::Response> resp =
      client.get(fullUrl)
        .timeout(std::chrono::milliseconds(HTTP_REQ_TIMEOUT))
        .send();

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
      Async::Throw);

    // Make calls synchronous
    Async::Barrier<Http::Response> barrier(resp);
    std::chrono::milliseconds timeout(HTTP_REQ_TIMEOUT);
    barrier.wait_for(timeout);

    client.shutdown();

    return std::make_pair((int)respCode, out.str());
}
}
