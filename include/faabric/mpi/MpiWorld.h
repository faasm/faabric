#pragma once

#include <faabric/mpi/MpiMessageBuffer.h>
#include <faabric/mpi/mpi.h>
#include <faabric/mpi/mpi.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#include <atomic>
#include <unordered_map>

// Constants for profiling MPI parameters like number of messages sent or
// message breakdown by type in the execution graph. Remember to increase the
// counter if you add another one
#define NUM_MPI_EXEC_GRAPH_DETAILS 2
#define MPI_MSG_COUNT_PREFIX "mpi-msgcount-torank"
#define MPI_MSGTYPE_COUNT_PREFIX "mpi-msgtype-torank"

namespace faabric::mpi {

struct MpiMessage {
    int32_t id;
    int32_t worldId;
    int32_t sendRank;
    int32_t recvRank;
    int32_t type;
    int32_t count;
    void* buffer;
};

// -----------------------------------
// Mocking
// -----------------------------------
// MPITOPTP - mocking at the MPI level won't be needed when using the PTP broker
// as the broker already has mocking capabilities
std::vector<std::shared_ptr<MPIMessage>> getMpiMockedMessages(int sendRank);

typedef faabric::util::FixedCapacityQueue<std::unique_ptr<MpiMessage>>
  InMemoryMpiQueue;

class MpiWorld
{
  public:
    MpiWorld();

    void create(faabric::Message& call, int newId, int newSize);

    void broadcastHostsToRanks();

    void initialiseFromMsg(faabric::Message& msg);

    std::string getHostForRank(int rank);

    std::string getUser();

    std::string getFunction();

    int getId() const;

    int getSize() const;

    void destroy();

    void getCartesianRank(int rank,
                          int maxDims,
                          const int* dims,
                          int* periods,
                          int* coords);

    void getRankFromCoords(int* rank, int* coords);

    void shiftCartesianCoords(int rank,
                              int direction,
                              int disp,
                              int* source,
                              int* destination);

    void send(int sendRank,
              int recvRank,
              const uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              MPIMessage::MPIMessageType messageType = MPIMessage::NORMAL);

    int isend(int sendRank,
              int recvRank,
              const uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              MPIMessage::MPIMessageType messageType = MPIMessage::NORMAL);

    void broadcast(int rootRank,
                   int thisRank,
                   uint8_t* buffer,
                   faabric_datatype_t* dataType,
                   int count,
                   MPIMessage::MPIMessageType messageType = MPIMessage::NORMAL);

    void recv(int sendRank,
              int recvRank,
              uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              MPI_Status* status,
              MPIMessage::MPIMessageType messageType = MPIMessage::NORMAL);

    int irecv(int sendRank,
              int recvRank,
              uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              MPIMessage::MPIMessageType messageType = MPIMessage::NORMAL);

    void awaitAsyncRequest(int requestId);

    void sendRecv(uint8_t* sendBuffer,
                  int sendcount,
                  faabric_datatype_t* sendDataType,
                  int sendRank,
                  uint8_t* recvBuffer,
                  int recvCount,
                  faabric_datatype_t* recvDataType,
                  int recvRank,
                  int myRank,
                  MPI_Status* status);

    void scatter(int sendRank,
                 int recvRank,
                 const uint8_t* sendBuffer,
                 faabric_datatype_t* sendType,
                 int sendCount,
                 uint8_t* recvBuffer,
                 faabric_datatype_t* recvType,
                 int recvCount);

    void gather(int sendRank,
                int recvRank,
                const uint8_t* sendBuffer,
                faabric_datatype_t* sendType,
                int sendCount,
                uint8_t* recvBuffer,
                faabric_datatype_t* recvType,
                int recvCount);

    void allGather(int rank,
                   const uint8_t* sendBuffer,
                   faabric_datatype_t* sendType,
                   int sendCount,
                   uint8_t* recvBuffer,
                   faabric_datatype_t* recvType,
                   int recvCount);

    void reduce(int sendRank,
                int recvRank,
                uint8_t* sendBuffer,
                uint8_t* recvBuffer,
                faabric_datatype_t* datatype,
                int count,
                faabric_op_t* operation);

    void allReduce(int rank,
                   uint8_t* sendBuffer,
                   uint8_t* recvBuffer,
                   faabric_datatype_t* datatype,
                   int count,
                   faabric_op_t* operation);

    void op_reduce(faabric_op_t* operation,
                   faabric_datatype_t* datatype,
                   int count,
                   uint8_t* inBuffer,
                   uint8_t* resultBuffer);

    void scan(int rank,
              uint8_t* sendBuffer,
              uint8_t* recvBuffer,
              faabric_datatype_t* datatype,
              int count,
              faabric_op_t* operation);

    void allToAll(int rank,
                  uint8_t* sendBuffer,
                  faabric_datatype_t* sendType,
                  int sendCount,
                  uint8_t* recvBuffer,
                  faabric_datatype_t* recvType,
                  int recvCount);

    void probe(int sendRank, int recvRank, MPI_Status* status);

    void barrier(int thisRank);

    std::shared_ptr<InMemoryMpiQueue> getLocalQueue(int sendRank, int recvRank);

    long getLocalQueueSize(int sendRank, int recvRank);

    void overrideHost(const std::string& newHost);

    double getWTime();

    std::vector<bool> getInitedRemoteMpiEndpoints();

    std::vector<bool> getInitedUMB();

    /* Profiling */

    void setMsgForRank(faabric::Message& msg);

    /* Function Migration */

    void prepareMigration(int thisRank);

  private:
    int id = -1;
    int size = -1;
    std::string thisHost;
    faabric::util::TimePoint creationTime;

    std::atomic_flag isDestroyed = false;

    std::string user;
    std::string function;

    std::vector<int> cartProcsPerDim;

    /* MPI internal messaging layer */

    // Track at which host each rank lives
    int getIndexForRanks(int sendRank, int recvRank) const;

    // Store the ranks that live in each host and host for each rank
    std::map<std::string, std::vector<int>> ranksForHost;
    std::vector<std::string> hostForRank;

    // Track local and remote leaders. The leader is stored in the first
    // position of the host to ranks map.
    int localLeader = -1;
    void initLocalRemoteLeaders();

    // In-memory queues for local messaging
    std::vector<std::shared_ptr<InMemoryMpiQueue>> localQueues;
    void initLocalQueues();

    // Remote messaging using the PTP layer
    faabric::transport::PointToPointBroker& broker;

    void sendRemoteMpiMessage(std::string dstHost,
                              int sendRank,
                              int recvRank,
                              const std::shared_ptr<MPIMessage>& msg);

    std::shared_ptr<MPIMessage> recvRemoteMpiMessage(int sendRank,
                                                     int recvRank);

    // Support for asyncrhonous communications
    std::shared_ptr<MpiMessageBuffer> getUnackedMessageBuffer(int sendRank,
                                                              int recvRank);

    std::shared_ptr<MPIMessage> recvBatchReturnLast(int sendRank,
                                                    int recvRank,
                                                    int batchSize = 0);

    /* Helper methods */

    void checkRanksRange(int sendRank, int recvRank);

    // Abstraction of the bulk of the recv work, shared among various functions
    void doRecv(std::shared_ptr<MPIMessage>& m,
                uint8_t* buffer,
                faabric_datatype_t* dataType,
                int count,
                MPI_Status* status,
                MPIMessage::MPIMessageType messageType = MPIMessage::NORMAL);

    // Abstraction of the bulk of the recv work, shared among various functions
    void doRecv(std::unique_ptr<MpiMessage> m,
                uint8_t* buffer,
                faabric_datatype_t* dataType,
                int count,
                MPI_Status* status,
                MPIMessage::MPIMessageType messageType = MPIMessage::NORMAL);
};
}
