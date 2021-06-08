#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/util/logging.h>

using namespace faabric::scheduler;

class ExampleExecutor : public Executor
{
  public:
    ExampleExecutor(const faabric::Message& msg)
      : Executor(msg)
    {}

    ~ExampleExecutor() {}

    int32_t executeTask(int threadPoolIdx,
                        int msgIdx,
                        std::shared_ptr<faabric::BatchExecuteRequest> req)
    {
        SPDLOG_INFO("Hello world!");
        faabric::Message& msg = req->mutable_messages()->at(msgIdx);
        msg.set_outputdata("This is hello output!");

        return 0;
    }
};

class ExampleExecutorFactory : public ExecutorFactory
{
  protected:
    std::shared_ptr<Executor> createExecutor(
      const faabric::Message& msg) override
    {
        return std::make_shared<ExampleExecutor>(msg);
    }
};

int main()
{
    // Start the worker pool
    SPDLOG_INFO("Starting executor pool in the background");
    std::shared_ptr<ExecutorFactory> fac =
      std::make_shared<ExampleExecutorFactory>();
    faabric::runner::FaabricMain m(fac);
    m.startBackground();

    // Start endpoint (will also have multiple threads)
    SPDLOG_INFO("Starting endpoint");
    faabric::endpoint::FaabricEndpoint endpoint;
    endpoint.start();

    SPDLOG_INFO("Shutting down endpoint");
    m.shutdown();

    return EXIT_SUCCESS;
}
