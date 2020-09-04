#pragma once

#include "FaabricPool.h"

#include <faabric/state/StateServer.h>
#include <faabric/util/config.h>

namespace faabric::executor {
    class FaabricMain {
    public:
        explicit FaabricMain(faabric::executor::FaabricPool &poolIn);

        void startBackground();

        void shutdown();
    private:
        faabric::util::SystemConfig &conf;
        faabric::scheduler::Scheduler &scheduler;

        faabric::executor::FaabricPool &pool;
    };
}