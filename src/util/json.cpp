#include <faabric/util/json.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#include <google/protobuf/util/json_util.h>

namespace faabric::util {
std::string messageToJson(const google::protobuf::Message& msg)
{
    std::string jsonStr;

    // Set the JSON print options. This is very important to ensure backwards-
    // compatibility with clients sending HTTP requests to faabric
    google::protobuf::util::JsonPrintOptions jsonPrintOptions;
    jsonPrintOptions.always_print_enums_as_ints = true;

    google::protobuf::util::Status status =
      google::protobuf::util::MessageToJsonString(msg, &jsonStr, jsonPrintOptions);
    if (!status.ok()) {
        SPDLOG_ERROR("Serialising JSON string to protobuf message: {}",
                     status.message().data());
        throw faabric::util::JsonSerialisationException(
          status.message().data());
    }

    return jsonStr;
}

void jsonToMessage(const std::string& jsonStr, google::protobuf::Message* msg)
{
    google::protobuf::util::JsonParseOptions jsonParseOptions;
    jsonParseOptions.ignore_unknown_fields = true;

    google::protobuf::util::Status status =
      google::protobuf::util::JsonStringToMessage(jsonStr, msg, jsonParseOptions);
    if (!status.ok()) {
        SPDLOG_ERROR("Deserialising JSON string to protobuf message: {}",
                     status.message().data());
        throw faabric::util::JsonSerialisationException(
          status.message().data());
    }
}
}
