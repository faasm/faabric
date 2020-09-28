#pragma once

#include "exception.h"
#include <proto/faabric.pb.h>

namespace faabric::util {
std::string postJsonFunctionCall(const std::string& host,
                                 int port,
                                 const faabric::Message& msg);

class ChainedCallFailedException : public faabric::util::FaabricException
{
  public:
    explicit ChainedCallFailedException(std::string message)
      : FaabricException(std::move(message))
    {}
};
}