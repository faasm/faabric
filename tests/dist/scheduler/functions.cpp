#include "faabric_utils.h"
#include <catch.hpp>

#include "DistTestExecutor.h"
#include "init.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/bytes.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>

namespace tests {

int handleSimpleThread(int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    auto const& logger = faabric::util::getLogger();

    // Return a distinctive value
    int returnValue = msg.id() / 2;

    const faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    logger->debug("Thread {} executed on host {}. Returning {}",
                  msg.id(),
                  conf.endpointHost,
                  returnValue);

    return returnValue;
}

int handleSimpleFunction(int threadPoolIdx,
                         int msgIdx,
                         std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);
    const faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    std::string output = fmt::format(
      "Function {} executed on host {}", msg.id(), conf.endpointHost);

    auto const& logger = faabric::util::getLogger();
    logger->debug(output);

    msg.set_outputdata(output);

    return 0;
}

int handleFakeDiffsFunction(int threadPoolIdx,
                            int msgIdx,
                            std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    std::string msgInput = msg.inputdata();
    std::string snapshotKey = msg.snapshotkey();

    // Create some fake diffs
    std::vector<uint8_t> inputBytes = faabric::util::stringToBytes(msgInput);
    std::vector<uint8_t> keyBytes = faabric::util::stringToBytes(snapshotKey);

    uint32_t offsetA = 10;
    uint32_t offsetB = 100;

    faabric::util::SnapshotDiff diffA(
      offsetA, keyBytes.data(), keyBytes.size());
    faabric::util::SnapshotDiff diffB(
      offsetB, inputBytes.data(), inputBytes.size());

    std::vector<faabric::util::SnapshotDiff> diffs = { diffA, diffB };

    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.setThreadResult(msg, 123, diffs);

    return 0;
}

void registerSchedulerTestFunctions()
{
    registerDistTestExecutorCallback("threads", "simple", handleSimpleThread);

    registerDistTestExecutorCallback("funcs", "simple", handleSimpleFunction);

    registerDistTestExecutorCallback(
      "snapshots", "fake-diffs", handleFakeDiffsFunction);
}
}
