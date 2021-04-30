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

    bool doExecute(faabric::Message& call)
    {
        auto logger = faabric::util::getLogger();

        logger->info("Hello world!");
        call.set_outputdata("This is hello output!");

        return true;
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
    const auto& logger = faabric::util::getLogger();

    // Start the worker pool
    logger->info("Starting executor pool in the background");
    std::shared_ptr<ExecutorFactory> fac =
      std::make_shared<ExampleExecutorFactory>();
    faabric::runner::FaabricMain m(fac);
    m.startBackground();

    // Start endpoint (will also have multiple threads)
    logger->info("Starting endpoint");
    faabric::endpoint::FaabricEndpoint endpoint;
    endpoint.start();

    logger->info("Shutting down endpoint");
    m.shutdown();

    return EXIT_SUCCESS;
}
