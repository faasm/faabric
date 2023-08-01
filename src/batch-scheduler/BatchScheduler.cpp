#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/BinPackScheduler.h>
#include <faabric/util/config.h>

namespace faabric::batch_scheduler {

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
        batchScheduler = std::make_shared<BinPackScheduler>(mode);
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
}
