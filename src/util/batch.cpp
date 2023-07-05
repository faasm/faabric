#include <faabric/util/batch.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>

namespace faabric::util {
std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory()
{
    auto req = std::make_shared<faabric::BatchExecuteRequest>();
    req->set_appid(faabric::util::generateGid());
    return req;
}

std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory(
  const std::string& user,
  const std::string& function,
  int count)
{
    auto req = batchExecFactory();

    // Force the messages to have the same app ID
    uint32_t appId = req->appid();
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

    for (int i = 1; i < ber->messages_size(); i++) {
        auto msg = ber->messages(i);
        if (msg.user() != user || msg.function() != func ||
            msg.appid() != appId) {
            return false;
        }
    }

    return true;
}
}
