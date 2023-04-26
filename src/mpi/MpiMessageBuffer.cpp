#include <faabric/mpi/MpiMessageBuffer.h>
#include <faabric/util/logging.h>

namespace faabric::mpi {
typedef std::list<MpiMessageBuffer::PendingAsyncMpiMessage>::iterator
  MpiMessageIterator;
bool MpiMessageBuffer::isEmpty()
{
    return pendingMsgs.empty();
}

int MpiMessageBuffer::size()
{
    return pendingMsgs.size();
}

void MpiMessageBuffer::addMessage(PendingAsyncMpiMessage msg)
{
    pendingMsgs.emplace_back(std::move(msg));
}

void MpiMessageBuffer::deleteMessage(const MpiMessageIterator& msgIt)
{
    pendingMsgs.erase(msgIt);
}

MpiMessageIterator MpiMessageBuffer::getRequestPendingMsg(int requestId)
{
    // The request id must be in the MMB, as an irecv must happen before an
    // await
    MpiMessageIterator msgIt =
      std::find_if(pendingMsgs.begin(),
                   pendingMsgs.end(),
                   [requestId](PendingAsyncMpiMessage pendingMsg) {
                       return pendingMsg.requestId == requestId;
                   });

    // If it's not there, error out
    if (msgIt == pendingMsgs.end()) {
        SPDLOG_ERROR("Asynchronous request id not in buffer: {}", requestId);
        throw std::runtime_error("Async request not in buffer");
    }

    return msgIt;
}

MpiMessageIterator MpiMessageBuffer::getFirstNullMsgUntil(
  const MpiMessageIterator& msgItEnd)
{
    return std::find_if(
      pendingMsgs.begin(), msgItEnd, [](PendingAsyncMpiMessage pendingMsg) {
          return pendingMsg.msg == nullptr;
      });
}

MpiMessageIterator MpiMessageBuffer::getFirstNullMsg()
{
    return getFirstNullMsgUntil(pendingMsgs.end());
}

int MpiMessageBuffer::getTotalUnackedMessagesUntil(
  const MpiMessageIterator& msgItEnd)
{
    MpiMessageIterator firstNull = getFirstNullMsgUntil(msgItEnd);
    return std::distance(firstNull, msgItEnd);
}

int MpiMessageBuffer::getTotalUnackedMessages()
{
    MpiMessageIterator firstNull = getFirstNullMsg();
    return std::distance(firstNull, pendingMsgs.end());
}
}
