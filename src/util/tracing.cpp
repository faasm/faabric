#include <faabric/util/logging.h>
#include <faabric/util/tracing.h>

namespace faabric::util::tracing {
void CallRecords::startRecording(const faabric::Message& msg)
{
#ifndef NDEBUG
    if (linkedMsg != nullptr && linkedMsg->id() != msg.id()) {
        SPDLOG_ERROR("CallRecords already linked to a different message: (linked: {} != provided: {})",
                     linkedMsg->id(), msg.id());
        throw std::runtime_error("CallRecords linked to a different message");
    } else if (linkedMsg == nullptr) {
        linkedMsg = std::make_shared<faabric::Message>(msg);
    }
#else
    ;
#endif
}

void CallRecords::stopRecording(faabric::Message& msg)
{
#ifndef NDEBUG
    if (linkedMsg == nullptr || linkedMsg->id() != msg.id()) {
        SPDLOG_ERROR("CallRecords not linked to the right message: (linked: {} != provided: {})",
                     linkedMsg->id(), msg.id());
        throw std::runtime_error("CallRecords linked to a different message");
    }

    linkedMsg = nullptr;

    // Update the actual faabric message
    faabric::CallRecords recordsMsg;
    for (const auto& recordType : onGoingRecordings) {
        loadRecordsToMessage(recordsMsg, recordType);
    }

    // Update the original message
    *msg.mutable_records() = recordsMsg;
#else
    ;
#endif
}

void CallRecords::loadRecordsToMessage(faabric::CallRecords& callRecords,
                                       const RecordType& recordType)
{
#ifndef NDEBUG
    switch (recordType) {
        case (faabric::util::tracing::RecordType::MpiPerRankMessageCount): {
            faabric::MpiPerRankMessageCount msgCount;

            for (const auto& it : perRankMsgCount) {
                msgCount.add_ranks(it.first);
                msgCount.add_nummessages(it.second);
            }

            *callRecords.mutable_mpimsgcount() = msgCount;
            break;
        }
        default: {
            SPDLOG_ERROR("Unsupported record type: {}", recordType);
            throw std::runtime_error("Unsupported record type");
        }
    }
#else
    ;
#endif
}

void CallRecords::addRecord(int msgId, RecordType recordType, int idToIncrement)
{
#ifndef NDEBUG
    // Check message id
    if (linkedMsg == nullptr || linkedMsg->id() != msgId) {
        SPDLOG_ERROR("CallRecords not linked to the right message: (linked: {} != provided: {})",
                     linkedMsg->id(), msgId);
        throw std::runtime_error("CallRecords linked to a different message");
    }

    // Add the record to the list of on going records if it is not there
    bool mustInit = false;
    auto it = std::find(onGoingRecordings.begin(), onGoingRecordings.end(), recordType);
    if (it == onGoingRecordings.end()) {
        onGoingRecordings.push_back(recordType);
        mustInit = true;
    }

    // Finally increment the corresponding record list
    switch (recordType) {
        case (faabric::util::tracing::RecordType::MpiPerRankMessageCount): {
            if (mustInit) {
                for (int i = 0; i < linkedMsg->mpiworldsize(); i++) {
                    perRankMsgCount[i] = 0;
                }
            }

            ++perRankMsgCount.at(idToIncrement);
            break;
        }
        default: {
            SPDLOG_ERROR("Unsupported record type: {}", recordType);
            throw std::runtime_error("Unsupported record type");
        }
    }
#else
    ;
#endif
}


CallRecords& getCallRecords()
{
    static thread_local CallRecords callRecords;
    return callRecords;
}
}
