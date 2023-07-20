#include <faabric/transport/Message.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <nng/nng.h>

namespace faabric::transport {

Message::Message(size_t bufferSize)
{
    if (int ec = nng_msg_alloc(&nngMsg, bufferSize); ec != 0) {
        SPDLOG_CRITICAL("Error allocating a message of size {}: {}",
                        bufferSize,
                        nng_strerror(ec));
        throw std::bad_alloc();
    }
}

Message::Message(nng_msg* nngMsg)
  : nngMsg(nngMsg)
{}

Message::Message(MessageResponseCode responseCodeIn)
  : responseCode(responseCodeIn)
{}

Message::~Message()
{
    if (nngMsg != nullptr) {
        nng_msg_free(nngMsg);
        nngMsg = nullptr;
    }
}

std::span<char> Message::data()
{
    auto udat = udata();
    return std::span(reinterpret_cast<char*>(udat.data()), udat.size_bytes());
}

std::span<const char> Message::data() const
{
    auto udat = udata();
    return std::span(reinterpret_cast<const char*>(udat.data()),
                     udat.size_bytes());
}

std::span<uint8_t> Message::udata()
{
    return allData().size() < HEADER_MSG_SIZE
             ? std::span<uint8_t>()
             : allData().subspan(HEADER_MSG_SIZE);
}

std::span<const uint8_t> Message::udata() const
{
    return allData().size() < HEADER_MSG_SIZE
             ? std::span<const uint8_t>()
             : allData().subspan(HEADER_MSG_SIZE);
}

std::vector<uint8_t> Message::dataCopy() const
{
    return std::vector<uint8_t>(udata().begin(), udata().end());
}
}
