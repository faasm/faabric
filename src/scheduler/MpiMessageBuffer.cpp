#include <faabric/scheduler/MpiMessageBuffer.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
typedef std::list<MpiMessageBuffer::Arguments>::iterator ArgListIterator;
bool MpiMessageBuffer::isEmpty()
{
    return args.empty();
}

int MpiMessageBuffer::size()
{
    return args.size();
}

void MpiMessageBuffer::addMessage(Arguments arg)
{
    args.push_back(arg);
}

void MpiMessageBuffer::deleteMessage(const ArgListIterator& argIt)
{
    args.erase(argIt);
}

ArgListIterator MpiMessageBuffer::getRequestArguments(int requestId)
{
    // The request id must be in the MMB, as an irecv must happen before an
    // await
    ArgListIterator argIt =
      std::find_if(args.begin(), args.end(), [requestId](Arguments args) {
          return args.requestId == requestId;
      });

    // If it's not there, error out
    if (argIt == args.end()) {
        SPDLOG_ERROR("Asynchronous request id not in buffer: {}", requestId);
        throw std::runtime_error("Async request not in buffer");
    }

    return argIt;
}

ArgListIterator MpiMessageBuffer::getFirstNullMsgUntil(
  const ArgListIterator& argItEnd)
{
    return std::find_if(args.begin(), argItEnd, [](Arguments args) {
        return args.msg == nullptr;
    });
}

ArgListIterator MpiMessageBuffer::getFirstNullMsg()
{
    return getFirstNullMsgUntil(args.end());
}

int MpiMessageBuffer::getTotalUnackedMessagesUntil(
  const ArgListIterator& argItEnd)
{
    ArgListIterator firstNull = getFirstNullMsgUntil(argItEnd);
    return std::distance(firstNull, argItEnd);
}

int MpiMessageBuffer::getTotalUnackedMessages()
{
    ArgListIterator firstNull = getFirstNullMsg();
    return std::distance(firstNull, args.end());
}
}
