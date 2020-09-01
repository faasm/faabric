#pragma once

#include <proto/faabric.pb.h>

#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/logging.h>

namespace faabric::executor {
    class FaabricExecutor {
    public:
        explicit FaabricExecutor(int threadIdxIn);


        void bindToFunction(const faabric::Message &msg, bool force = false);

        void run();

        bool isBound();

        virtual std::string processNextMessage();

        std::string executeCall(faabric::Message &call);

        void finish();

        std::string id;

        const int threadIdx;
    protected:
        virtual void postBind(const faabric::Message &msg, bool force);

        virtual bool doExecute(const faabric::Message &msg);

        virtual void postFinishCall(faabric::Message &call, bool success, const std::string &errorMsg);

        virtual void postFinish();

        virtual void flush();
    private:
        bool _isBound = false;

        faabric::scheduler::Scheduler &scheduler;

        faabric::Message boundMessage;

        int executionCount = 0;

        std::shared_ptr<faabric::scheduler::InMemoryMessageQueue> currentQueue;

        void finishCall(faabric::Message &msg, bool success, const std::string &errorMsg);
    };
}