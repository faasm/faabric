#include <faabric/util/batch.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>

namespace faabric::util {
std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory()
{
    auto req = std::make_shared<faabric::BatchExecuteRequest>();
    req->set_id(generateGid());
    return req;
}

std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory(
  const std::string& user,
  const std::string& function,
  int count)
{
    auto req = batchExecFactory();

    // Force the messages to have the same app ID than the BER
    int appId = req->id();
    for (int i = 0; i < count; i++) {
        *req->add_messages() = messageFactory(user, function);
        req->mutable_messages()->at(i).set_appid(appId);
    }

    return req;
}

bool isBatchExecRequestValid(std::shared_ptr<faabric::BatchExecuteRequest> ber)
{
    if (ber == nullptr) {
        return false;
    }

    // An empty BER (thus invalid) will have 0 messages and an id of 0
    if (ber->messages_size() <= 0 && ber->appid() == 0) {
        return false;
    }

    std::string user = ber->messages(0).user();
    std::string func = ber->messages(0).function();
    int appId = ber->messages(0).appid();

    // If the user or func are empty, the BER is invalid
    if (user.empty() || func.empty()) {
        return false;
    }

    // The BER and all messages must have the same appid
    if (ber->appid() != appId) {
        return false;
    }

    // All messages in the BER must have the same app id, user, and function
    for (int i = 1; i < ber->messages_size(); i++) {
        auto msg = ber->messages(i);
        if (msg.user() != user || msg.function() != func ||
            msg.appid() != appId) {
            return false;
        }
    }

    return true;
}

std::shared_ptr<faabric::BatchExecuteRequestStatus>
batchExecStatusFactory(int32_t appId)
{
    auto berStatus = std::make_shared<faabric::BatchExecuteRequestStatus>();
    berStatus->set_appid(appId);
    berStatus->set_finished(false);

    return berStatus;
}

std::shared_ptr<faabric::BatchExecuteRequestStatus>
batchExecStatusFactory(std::shared_ptr<faabric::BatchExecuteRequest> ber)
{
    return batchExecStatusFactory(ber->appid());
}
}
