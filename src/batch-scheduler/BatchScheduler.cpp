#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/BinPackScheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

namespace faabric::batch_scheduler {

void printHostMap(HostMap hostMap, const std::string& logLevel)
{
    std::string printedText;
    std::string header = "\n-------------- Host Map --------------";
    std::string subhead = "Ip\t\tSlots";
    std::string footer = "--------------------------------------";

    printedText += header + "\n" + subhead + "\n";
    for (const auto& [ip, hostState] : hostMap) {
        printedText += fmt::format(
          "{}\t\t{}/{}\n", ip, hostState->usedSlots, hostState->slots);
    }
    printedText += footer;

    SPDLOG_DEBUG(printedText);
}

// We have one static instance of the BatchScheduler globally. Note that the
// BatchScheduler is stateless, so having one static instance is very much like
// having a C++ interface
static std::shared_ptr<BatchScheduler> batchScheduler = nullptr;

std::shared_ptr<BatchScheduler> getBatchScheduler()
{
    if (batchScheduler != nullptr) {
        return batchScheduler;
    }

    std::string mode = faabric::util::getSystemConfig().batchSchedulerMode;

    if (mode == "bin-pack") {
        batchScheduler = std::make_shared<BinPackScheduler>();
    } else {
        SPDLOG_ERROR("Unrecognised batch scheduler mode: {}", mode);
        throw std::runtime_error("Unrecognised batch scheduler mode");
    }

    return batchScheduler;
}

void resetBatchScheduler()
{
    batchScheduler = nullptr;
}

DecisionType BatchScheduler::getDecisionType(
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    int appId = req->appid();

    if (!inFlightReqs.contains(appId)) {
        return DecisionType::NEW;
    }

    if (req->type() == BatchExecuteRequest_BatchExecuteType_MIGRATION) {
        return DecisionType::DIST_CHANGE;
    }

    return DecisionType::SCALE_CHANGE;
}
}
