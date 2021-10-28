#pragma once

#include <faabric/proto/faabric.pb.h>

#include <functional>
#include <list>
#include <map>

namespace faabric::util {
class ExecGraphDetail
{
  public:
    void startRecording(const faabric::Message& msg);

    void stopRecording(faabric::Message& msg);

    void addDetail(const int msgId,
                   const std::string& key,
                   const std::string& value);

    void incrementCounter(const int msgId,
                          const std::string& key,
                          const int valueToIncrement = 1);

    static inline std::string const mpiMsgCountPrefix = "mpi-msgcount-torank-";

  private:
    std::shared_ptr<faabric::Message> linkedMsg = nullptr;

    std::map<std::string, std::string> detailsMap;

    std::map<std::string, int> intDetailsMap;

    void checkMessageLinked(const int msgId);

    void checkMessageNotLinked();

    // ----- Wrappers to no-op the functions if not recording -----

    std::function<void(const int, const std::string&, const std::string&)>
      doAddDetail;

    void addDetailInternal(const int msgId,
                           const std::string& key,
                           const std::string& value);

    std::function<void(const int, const std::string&, const int)>
      doIncrementCounter;

    void incrementCounterInternal(const int msgId,
                                  const std::string& key,
                                  const int valueToIncrement);
};

ExecGraphDetail& getExecGraphDetail();
}
