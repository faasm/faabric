#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/exception.h>

namespace faabric::util {
std::string messageToJson(const faabric::Message& msg);

faabric::Message jsonToMessage(const std::string& jsonIn);

class JsonFieldNotFound : public faabric::util::FaabricException
{
  public:
    explicit JsonFieldNotFound(std::string message)
      : FaabricException(std::move(message))
    {}
};

std::string getJsonOutput(const faabric::Message& msg);

std::string getValueFromJsonString(const std::string& key,
                                   const std::string& jsonIn);
}
