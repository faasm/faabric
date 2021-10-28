#include <faabric/util/exec_graph.h>
#include <faabric/util/logging.h>
#include <faabric/util/testing.h>

namespace faabric::util {
void ExecGraphDetail::startRecording(const faabric::Message& msg)
{
    // In the tests there's not a thread to message mapping as we sometimes
    // spawn extra threads to mock work. Thus, we skip this check here.
    if (faabric::util::isTestMode()) {
        return;
    }

    checkMessageNotLinked();

    linkedMsg = std::make_shared<faabric::Message>(msg);

    if (!msg.recordexecgraph()) {
        doAddDetail =
          [](const int, const std::string&, const std::string&) -> void { ; };
        doIncrementCounter =
          [](const int, const std::string&, const int) -> void { ; };
    } else {
        doAddDetail = [this](const int msgId,
                             const std::string& key,
                             const std::string& value) -> void {
            this->addDetailInternal(msgId, key, value);
        };
        doIncrementCounter = [this](const int msgId,
                                    const std::string& key,
                                    const int valueToIncrement) -> void {
            this->incrementCounterInternal(msgId, key, valueToIncrement);
        };
    }
}

void ExecGraphDetail::stopRecording(faabric::Message& msg)
{
    // In the tests there's not a thread to message mapping as we sometimes
    // spawn extra threads to mock work. Thus, we skip this check here.
    if (faabric::util::isTestMode()) {
        return;
    }

    checkMessageLinked(msg.id());

    for (const auto& it : detailsMap) {
        faabric::ExecGraphDetail detail;
        detail.set_key(it.first);
        detail.set_value(it.second);
        *msg.add_execgraphdetails() = detail;
        SPDLOG_TRACE("Adding exec. graph detail to message. id: {} ; {}->{}",
                     linkedMsg->id(),
                     it.first,
                     it.second);
    }

    for (const auto& it : intDetailsMap) {
        if (detailsMap.find(it.first) != detailsMap.end()) {
            SPDLOG_WARN(
              "Replicated key in the exec graph details: {}->{} and {}->{}",
              it.first,
              detailsMap.at(it.first),
              it.first,
              it.second);
        }

        faabric::ExecGraphDetail detail;
        detail.set_key(it.first);
        detail.set_value(std::to_string(it.second));
        *msg.add_execgraphdetails() = detail;
        SPDLOG_TRACE("Adding exec. graph detail to message. id: {} ; {}->{}",
                     linkedMsg->id(),
                     it.first,
                     it.second);
    }

    linkedMsg = nullptr;
    detailsMap.clear();
    intDetailsMap.clear();
}

void ExecGraphDetail::checkMessageLinked(const int msgId)
{
    if (linkedMsg == nullptr || linkedMsg->id() != msgId) {
        SPDLOG_ERROR("Error during recording, records not linked to the right"
                     " message: (linked: {} != provided: {})",
                     linkedMsg == nullptr ? "nullptr"
                                          : std::to_string(linkedMsg->id()),
                     msgId);
        throw std::runtime_error("CallRecords linked to a different message");
    }
}

void ExecGraphDetail::checkMessageNotLinked()
{
    if (linkedMsg != nullptr) {
        SPDLOG_ERROR("Error starting recording, record already linked to"
                     "another message: {}",
                     linkedMsg->id());
        throw std::runtime_error("CallRecords linked to a different message");
    }
}

void ExecGraphDetail::addDetail(const int msgId,
                                const std::string& key,
                                const std::string& value)
{
    doAddDetail(msgId, key, value);
}

void ExecGraphDetail::addDetailInternal(const int msgId,
                                        const std::string& key,
                                        const std::string& value)
{
    if (faabric::util::isTestMode()) {
        return;
    }

    checkMessageLinked(msgId);

    detailsMap[key] = value;
}

void ExecGraphDetail::incrementCounter(const int msgId,
                                       const std::string& key,
                                       const int valueToIncrement)
{
    doIncrementCounter(msgId, key, valueToIncrement);
}

void ExecGraphDetail::incrementCounterInternal(const int msgId,
                                               const std::string& key,
                                               const int valueToIncrement)
{
    if (faabric::util::isTestMode()) {
        return;
    }

    checkMessageLinked(msgId);

    intDetailsMap[key] += valueToIncrement;
}

ExecGraphDetail& getExecGraphDetail()
{
    static thread_local ExecGraphDetail graphDetail;
    return graphDetail;
}
}
