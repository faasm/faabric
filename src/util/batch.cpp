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
}
