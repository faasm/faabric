#include "faabric_utils.h"
#include <catch.hpp>

#include "DistTestExecutor.h"
#include "init.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>

namespace tests {

int handleSimpleThread(int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    auto const& logger = faabric::util::getLogger();

    const faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    logger->debug("Thread {} executed on host {}", msg.id(), conf.endpointHost);

    // Return a distinctive value
    return 12 * msg.id();
}

int handleSimpleFunction(int threadPoolIdx,
                         int msgIdx,
                         std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);
    const faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    std::string output = fmt::format(
      "Function {} executed on host {}", msg.id(), conf.endpointHost);

    msg.set_outputdata(output);

    return 0;
}

void registerThreadFunctions()
{
    registerDistTestExecutorCallback("threads", "simple", handleSimpleThread);
    registerDistTestExecutorCallback("funcs", "simple", handleSimpleFunction);
}
}
