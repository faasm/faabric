#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/exception.h>

#include <google/protobuf/util/json_util.h>

namespace faabric::util {
void messageToJsonPb(const google::protobuf::Message& msg, std::string* jsonStr);

void jsonToMessagePb(const std::string& jsonStr, google::protobuf::Message* msg);

class JsonSerialisationException : public faabric::util::FaabricException
{
  public:
    explicit JsonSerialisationException(std::string message)
      : FaabricException(std::move(message))
    {}
};

// TODO: the old Json messages relying on protobuf are very embedded through
// many repositories interacting with faasm's http API, so be careful when
// removing them

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
