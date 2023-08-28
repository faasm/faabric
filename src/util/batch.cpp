#include <faabric/util/batch.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>

namespace faabric::util {
std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory()
{
    auto req = std::make_shared<faabric::BatchExecuteRequest>();
    req->set_appid(generateGid());
    return req;
}

std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory(
  const std::string& user,
  const std::string& function,
  int count)
{
    auto req = batchExecFactory();
    req->set_user(user);
    req->set_function(function);

    // Force the messages to have the same app ID than the BER
    int appId = req->appid();
    for (int i = 0; i < count; i++) {
        *req->add_messages() = messageFactory(user, function);
        req->mutable_messages()->at(i).set_appid(appId);
    }

    return req;
}

bool isBatchExecRequestValid(std::shared_ptr<faabric::BatchExecuteRequest> ber)
{
    if (ber == nullptr) {
        SPDLOG_ERROR("Ber points to null!");
        return false;
    }

    // An empty BER (thus invalid) will have 0 messages and an id of 0
    if (ber->messages_size() <= 0 && ber->appid() == 0) {
        SPDLOG_ERROR("Invalid (uninitialised) BER (size: {} - app id: {})",
                     ber->messages_size(),
                     ber->appid());
        return false;
    }

    std::string user = ber->user();
    std::string func = ber->function();
    int appId = ber->appid();

    // If the user or func are empty, the BER is invalid
    if (user.empty() || func.empty()) {
        SPDLOG_ERROR("Unset user ({}) or func ({}) in BER!", user, func);
        return false;
    }

    // All messages in the BER must have the same app id, user, and function
    for (int i = 0; i < ber->messages_size(); i++) {
        auto msg = ber->messages(i);
        if (msg.user() != user || msg.function() != func ||
            msg.appid() != appId) {
            SPDLOG_ERROR("Malformed message in BER");
            SPDLOG_ERROR("Got: (id: {} - user: {} - func: {} - app: {})",
                         msg.id(),
                         msg.user(),
                         msg.function(),
                         msg.appid());
            SPDLOG_ERROR("Expected: (id: {} - user: {} - func: {} - app: {})",
                         msg.id(),
                         user,
                         func,
                         appId);
            return false;
        }
    }

    return true;
}

void updateBatchExecAppId(std::shared_ptr<faabric::BatchExecuteRequest> ber,
                          int newAppId)
{
    ber->set_appid(newAppId);
    for (int i = 0; i < ber->messages_size(); i++) {
        ber->mutable_messages(i)->set_appid(newAppId);
    }

    // Sanity-check in debug mode
    assert(isBatchExecRequestValid(ber));
}

void updateBatchExecGroupId(std::shared_ptr<faabric::BatchExecuteRequest> ber,
                            int newGroupId)
{
    ber->set_groupid(newGroupId);
    for (int i = 0; i < ber->messages_size(); i++) {
        ber->mutable_messages(i)->set_groupid(newGroupId);
    }

    // Sanity-check in debug mode
    assert(isBatchExecRequestValid(ber));
}

std::shared_ptr<faabric::BatchExecuteRequestStatus> batchExecStatusFactory(
  int32_t appId)
{
    auto berStatus = std::make_shared<faabric::BatchExecuteRequestStatus>();
    berStatus->set_appid(appId);
    berStatus->set_finished(false);

    return berStatus;
}

std::shared_ptr<faabric::BatchExecuteRequestStatus> batchExecStatusFactory(
  std::shared_ptr<faabric::BatchExecuteRequest> ber)
{
    return batchExecStatusFactory(ber->appid());
}
}
