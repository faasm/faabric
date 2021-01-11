#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/executor/FaabricExecutor.h>
#include <faabric/executor/FaabricPool.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/logging.h>

#include <faabric/mpi/mpi.h>
#include <faabric/mpi/MpiExecutor.h>

#include <unistd.h>

#if 0
#define FAABRIC_MPI_MAIN()
    using namespace faabric::executor;

    bool _execMpiFunc(const faabric::Message* msg);

    class _Executor final : public FaabricExecutor
    {
      public:
        explicit _Executor()
          : FaabricExecutor(0)
        {}

        void setExecutingCall(faabric::Message* msg) {
            this->m_executingCall = msg;
        }

        bool doExecute(faabric::Message& msg) override { 
            setExecutingCall(&msg);
            return _execMpiFunc(&msg);
        }

        bool postFinishCall() override {
            auto logger = faabric::util::getLogger();
            logger->debug("Finished MPI execution.");
        }

        faabric::Message* getExecutingCall() { return m_executingCall; }

      private:
        faabric::Message* m_executingCall;
    };
    class _SingletonPool : public FaabricPool
    {
      public:
        explicit _SingletonPool()
          : FaabricPool(1)
        {}

        std::unique_ptr<FaabricExecutor> createExecutor(int threadIdx) override
        {
            return std::make_unique<_Executor>();
        }
    };
    int main(int argc, char** argv)
    {
        auto logger = faabric::util::getLogger();
        faabric::scheduler::Scheduler& scheduler = faabric::scheduler::getScheduler();
        auto conf = faabric::util::getSystemConfig();
        faabric::endpoint::FaabricEndpoint endpoint;

        // Add host to the global set
        scheduler.addHostToGlobalSet();

        // Print current configuration
        conf.print();

        // Start the thread pool (nThreads == 1) and the state and function
        // call servers.
        _SingletonPool p;
        p.startThreadPool();
        p.startStateServer();
        p.startFunctionCallServer();
        endpoint.start();

        // Shutdown
        scheduler.clear();
        p.shutdown();
    }

    bool _execMpiFunc(const faabric::Message* msg)
#endif

using namespace faabric::executor;

FAABRIC_MPI_MAIN()
{
    auto logger = faabric::util::getLogger();
    logger->info("Hello world from Faabric MPI Main!");
    logger->info("this is our executing call {}", msg->user());

    MPI_Init(NULL, NULL);

    int rank, worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    sleep(2);

    MPI_Finalize();

    return 0;
}
