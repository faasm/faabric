#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/random.h>

namespace faabric::util {

std::vector<uint8_t> messageToBytes(const faabric::Message& msg)
{
    size_t byteSize = msg.ByteSizeLong();
    std::vector<uint8_t> inputData(byteSize, 0);
    msg.SerializeToArray(inputData.data(), (int)inputData.size());

    return inputData;
}

std::string funcToString(const faabric::Message& msg, bool includeId)
{
    std::string str = msg.user() + "/" + msg.function();

    if (includeId) {
        str += ":" + std::to_string(msg.id());
    }

    if (msg.issgx()) {
        str += ":sgx";
    }

    return str;
}

std::string funcToString(
  const std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return funcToString(req->messages(0), false);
}

std::string buildAsyncResponse(const faabric::Message& msg)
{
    if (msg.id() == 0) {
        throw std::runtime_error(
          "Message must have id to build async response");
    }

    return std::to_string(msg.id());
}

std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory()
{
    auto req = std::make_shared<faabric::BatchExecuteRequest>();
    req->set_id(faabric::util::generateGid());
    return req;
}

std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory(
  const std::string& user,
  const std::string& function,
  int count)
{
    auto req = batchExecFactory();

    for (int i = 0; i < count; i++) {
        *req->add_messages() = messageFactory(user, function);
    }

    return req;
}

std::shared_ptr<faabric::Message> messageFactoryShared(
  const std::string& user,
  const std::string& function)
{
    auto ptr = std::make_shared<faabric::Message>();

    ptr->set_user(user);
    ptr->set_function(function);

    setMessageId(*ptr);

    std::string thisHost = faabric::util::getSystemConfig().endpointHost;
    ptr->set_masterhost(thisHost);
    return ptr;
}

faabric::Message messageFactory(const std::string& user,
                                const std::string& function)
{
    faabric::Message msg;
    msg.set_user(user);
    msg.set_function(function);

    setMessageId(msg);

    std::string thisHost = faabric::util::getSystemConfig().endpointHost;
    msg.set_masterhost(thisHost);

    return msg;
}

unsigned int setMessageId(faabric::Message& msg)
{
    // If message already has an ID, just make sure the keys are set up
    unsigned int messageId;
    if (msg.id() > 0) {
        messageId = msg.id();
    } else {
        // Generate a random ID
        messageId = faabric::util::generateGid();
        msg.set_id(messageId);
    }

    // Set an app ID if not already set
    if (msg.appid() == 0) {
        msg.set_appid(faabric::util::generateGid());
    }

    // Set the timestamp if it doesn't have one
    if (msg.timestamp() <= 0) {
        Clock& clock = faabric::util::getGlobalClock();
        msg.set_timestamp(clock.epochMillis());
    }

    std::string resultKey = resultKeyFromMessageId(messageId);
    msg.set_resultkey(resultKey);

    std::string statusKey = statusKeyFromMessageId(messageId);
    msg.set_statuskey(statusKey);

    return messageId;
}

std::string resultKeyFromMessageId(unsigned int mid)
{
    std::string k = "result_";
    k += std::to_string(mid);
    return k;
}

std::string statusKeyFromMessageId(unsigned int mid)
{
    std::string k = "status_";
    k += std::to_string(mid);
    return k;
}

std::vector<std::string> getArgvForMessage(const faabric::Message& msg)
{
    // We always have some arbitrary script name as argv[0]
    std::vector<std::string> argv = { "function.wasm" };

    std::string cmdlineArgs = msg.cmdline();
    if (cmdlineArgs.empty()) {
        return argv;
    }

    // Split the extra args
    std::vector<std::string> extraArgs;
    std::string copy(msg.cmdline());
    boost::split(extraArgs, copy, [](char c) { return c == ' '; });

    // Build the final list
    argv.insert(argv.end(), extraArgs.begin(), extraArgs.end());

    return argv;
}
}
