#pragma once

#include <faabric/proto/faabric.pb.h>

#include <list>
#include <map>

namespace faabric::util::tracing {
enum RecordType
{
    MpiPerRankMessageCount
};

class CallRecords
{
  public:
    void startRecording(const faabric::Message& msg);

    void stopRecording(faabric::Message& msg);

    void addRecord(int msgId, RecordType recordType, int idToIncrement);

  private:
    std::shared_ptr<faabric::Message> linkedMsg = nullptr;

    std::list<RecordType> onGoingRecordings;

    void loadRecordsToMessage(faabric::CallRecords& callRecords,
                              const RecordType& recordType);

    // ----- Per record type data structures -----

    std::map<int, int> perRankMsgCount;
};

CallRecords& getCallRecords();
}
