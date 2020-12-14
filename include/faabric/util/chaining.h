#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/exception.h>

namespace faabric::util {
class ChainedCallFailedException : public faabric::util::FaabricException
{
  public:
    explicit ChainedCallFailedException(std::string message)
      : FaabricException(std::move(message))
    {}
};
}
