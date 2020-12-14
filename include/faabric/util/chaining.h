#pragma once

#include <faabric/util/exception.h>
#include <faabric/proto/faabric.pb.h>

namespace faabric::util {
class ChainedCallFailedException : public faabric::util::FaabricException
{
  public:
    explicit ChainedCallFailedException(std::string message)
      : FaabricException(std::move(message))
    {}
};
}
