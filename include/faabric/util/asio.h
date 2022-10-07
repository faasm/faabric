#pragma once

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

namespace asio = boost::asio;
namespace beast = boost::beast;

namespace faabric::util {
using BeastHttpRequest = beast::http::request<beast::http::string_body>;
using BeastHttpResponse = beast::http::response<beast::http::string_body>;
}
