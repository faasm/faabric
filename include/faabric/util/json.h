#pragma once

#include <faabric/util/exception.h>

#include <google/protobuf/util/json_util.h>

namespace faabric::util {
std::string messageToJson(const google::protobuf::Message& msg);

void jsonToMessage(const std::string& jsonStr, google::protobuf::Message* msg);

class JsonSerialisationException : public faabric::util::FaabricException
{
  public:
    explicit JsonSerialisationException(std::string message)
      : FaabricException(std::move(message))
    {}
};
}
