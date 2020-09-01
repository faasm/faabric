#pragma once

#include "FaabricPool.h"

#include <faabric/state/StateServer.h>
#include <faabric/util/config.h>

namespace faabric::executor {
    class FaabricMain {
    public:
        FaabricMain();

        void startBackground();

        void shutdown();
    private:
        faabric::util::SystemConfig &conf;
        faabric::executor::FaabricPool pool;
        faabric::scheduler::Scheduler &scheduler;
    };
}