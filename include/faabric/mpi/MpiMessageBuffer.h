#include <faabric/mpi/mpi.h>
#include <faabric/mpi/mpi.pb.h>

#include <iterator>
#include <list>

namespace faabric::mpi {
/* The MPI message buffer (MMB) keeps track of the asyncrhonous
 * messages that we must have received (i.e. through an irecv call) but we
 * still have not waited on (acknowledged). Messages are acknowledged either
 * through a call to recv or a call to await. A call to recv will
 * acknowledge (i.e. synchronously read from transport buffers) as many
 * unacknowleged messages there are. A call to await with a request
 * id as a parameter will acknowledge as many unacknowleged messages there are
 * until said request id.
 */
class MpiMessageBuffer
{
  public:
    /* This structure holds the metadata for each Mpi message we keep in the
     * buffer. Note that the message field will point to null if unacknowleged
     * or to a valid message otherwise.
     */
    class PendingAsyncMpiMessage
    {
      public:
        int requestId = -1;
        std::shared_ptr<MPIMessage> msg = nullptr;
        int sendRank = -1;
        int recvRank = -1;
        uint8_t* buffer = nullptr;
        faabric_datatype_t* dataType = nullptr;
        int count = -1;
        MPIMessage::MPIMessageType messageType = faabric::MPIMessage::NORMAL;

        bool isAcknowledged() { return msg != nullptr; }

        void acknowledge(std::shared_ptr<MPIMessage> msgIn)
        {
            msg = msgIn;
        }
    };

    /* Interface to query the buffer size */

    bool isEmpty();

    int size();

    /* Interface to add and delete messages to the buffer */

    void addMessage(PendingAsyncMpiMessage msg);

    void deleteMessage(
      const std::list<PendingAsyncMpiMessage>::iterator& msgIt);

    /* Interface to get a pointer to a message in the MMB */

    // Pointer to a message given its request id
    std::list<PendingAsyncMpiMessage>::iterator getRequestPendingMsg(
      int requestId);

    // Pointer to the first null-pointing (unacknowleged) message
    std::list<PendingAsyncMpiMessage>::iterator getFirstNullMsg();

    /* Interface to ask for the number of unacknowleged messages */

    // Unacknowledged messages until an iterator (used in await)
    int getTotalUnackedMessagesUntil(
      const std::list<PendingAsyncMpiMessage>::iterator& msgIt);

    // Unacknowledged messages in the whole buffer (used in recv)
    int getTotalUnackedMessages();

  private:
    std::list<PendingAsyncMpiMessage> pendingMsgs;

    std::list<PendingAsyncMpiMessage>::iterator getFirstNullMsgUntil(
      const std::list<PendingAsyncMpiMessage>::iterator& msgIt);
};
}
