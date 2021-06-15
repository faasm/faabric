#include <faabric/mpi/mpi.h>
#include <faabric/proto/faabric.pb.h>

#include <iterator>
#include <list>

namespace faabric::scheduler {
/* The MPI message buffer (MMB) keeps track of the asyncrhonous
 * messages that we must have received (i.e. through an irecv call) but we
 * still have not waited on (acknowledged). Messages are acknowledged either
 * through a call to recv or a call to await. A call to recv will
 * acknowledge (i.e. synchronously read from transport buffers) as many
 * unacknowleged messages there are, plus one.
 */
class MpiMessageBuffer
{
  public:
    struct Arguments
    {
        int requestId;
        std::shared_ptr<faabric::MPIMessage> msg;
        int sendRank;
        int recvRank;
        uint8_t* buffer;
        faabric_datatype_t* dataType;
        int count;
        faabric::MPIMessage::MPIMessageType messageType;
    };

    void addMessage(Arguments arg);

    void deleteMessage(const std::list<Arguments>::iterator& argIt);

    bool isEmpty();

    int size();

    std::list<Arguments>::iterator getRequestArguments(int requestId);

    std::list<Arguments>::iterator getFirstNullMsgUntil(
      const std::list<Arguments>::iterator& argIt);

    std::list<Arguments>::iterator getFirstNullMsg();

    int getTotalUnackedMessagesUntil(
      const std::list<Arguments>::iterator& argIt);

    int getTotalUnackedMessages();

  private:
    // We keep track of the request id and its arguments. Note that the message
    // is part of the arguments and may be null if unacknowleged.
    std::list<Arguments> args;
};
}
