#include <faabric/scheduler/MpiMessageBuffer.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
typedef std::list<MpiMessageBuffer::Arguments>::iterator ArgListIterator;
void MpiMessageBuffer::addMessage(Arguments arg)
{
    // Ensure we are enqueueing a null message (i.e. unacknowleged)
    assert(arg.msg == nullptr);

    args.push_back(arg);
}

void MpiMessageBuffer::deleteMessage(const ArgListIterator& argIt)
{
    args.erase(argIt);
    return;
}

bool MpiMessageBuffer::isEmpty()
{
    return args.empty();
}

int MpiMessageBuffer::size()
{
    return args.size();
}

ArgListIterator MpiMessageBuffer::getRequestArguments(int requestId)
{
    // The request id must be in the UMB, as an irecv must happen before an
    // await
    ArgListIterator argIt =
      std::find_if(args.begin(), args.end(), [requestId](Arguments args) {
          return args.requestId == requestId;
      });

    // If it's not there, error out
    if (argIt == args.end()) {
        SPDLOG_ERROR("Asynchronous request id not in UMB: {}", requestId);
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
