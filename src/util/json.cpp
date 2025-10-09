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
    google::protobuf::util::JsonPrintOptions jsonOptions;
    jsonOptions.always_print_enums_as_ints = true;

    auto status =
      google::protobuf::util::MessageToJsonString(msg, &jsonStr, jsonOptions);
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
    auto status = google::protobuf::util::JsonStringToMessage(jsonStr, msg);
    if (!status.ok()) {
        SPDLOG_ERROR("Deserialising JSON string to protobuf message: {}",
                     status.message().data());
        throw faabric::util::JsonSerialisationException(
          status.message().data());
    }
}
}
