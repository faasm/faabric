#pragma once

#include "exception.h"
#include <proto/faabric.pb.h>

namespace faabric::util {
class ChainedCallFailedException : public faabric::util::FaabricException
{
  public:
    explicit ChainedCallFailedException(std::string message)
      : FaabricException(std::move(message))
    {}
};
}
