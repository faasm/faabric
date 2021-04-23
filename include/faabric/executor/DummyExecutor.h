#include <faabric/executor/FaabricExecutor.h>

namespace faabric::executor {

class DummyExecutor final : public FaabricExecutor
{
  public:
    explicit DummyExecutor(int threadIdx);

    void flush() override;

  protected:
    void postBind(const faabric::Message& msg, bool force) override;

    bool doExecute(faabric::Message& call) override;

    std::future<int32_t> doBatchExecuteThread(
      int threadPoolIdx,
      const faabric::Message& msg) override;

    void preFinishCall(faabric::Message& call,
                       bool success,
                       const std::string& errorMsg) override;

    void postFinish() override;
};

}
