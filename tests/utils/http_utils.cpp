#include <faabric/util/bytes.h>
#include <faabric/util/logging.h>

#include <curl/curl.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#define HTTP_REQ_TIMEOUT 5000

namespace tests {

static size_t writerCallback(void* contents,
                             size_t elemSize,
                             size_t nElems,
                             void* arg)
{
    size_t contentSize = elemSize * nElems;
    auto* resp = (std::vector<char>*)arg;

    size_t initialSize = resp->size();
    resp->resize(initialSize + contentSize + 1);

    std::copy((char*)contents,
              ((char*)contents) + contentSize,
              resp->data() + initialSize);

    resp->at(initialSize + contentSize) = 0;

    return contentSize;
}

std::pair<int, std::string> postToUrl(const std::string& host,
                                      int port,
                                      const std::string& body)
{
    curl_global_init(CURL_GLOBAL_ALL);

    std::string fullUrl = fmt::format("{}:{}", host, port);
    SPDLOG_TRACE("Making HTTP POST request to {}", fullUrl);

    CURL* curl = curl_easy_init();
    if (curl == nullptr) {
        throw std::runtime_error("Failed to initialise libcurl");
    }

    // Url
    curl_easy_setopt(curl, CURLOPT_URL, fullUrl.c_str());

    // Writer
    std::vector<char> respData;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writerCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)&respData);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "faabric-test/1.0");

    // Post data
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, body.size());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());

    // Make the request
    CURLcode res = curl_easy_perform(curl);

    // Get response code and response body
    long respCode = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &respCode);
    std::string out(respData.data());

    SPDLOG_TRACE("HTTP response {}: {}", respCode, out);

    if (res != CURLE_OK) {
        SPDLOG_ERROR(
          "Getting from {} failed: {}", fullUrl, curl_easy_strerror(res));
    }

    curl_easy_cleanup(curl);
    curl_global_cleanup();

    return std::make_pair((int)respCode, out.c_str());
}
}
