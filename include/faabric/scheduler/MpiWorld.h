#pragma once

#include <faabric/mpi/mpi.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>
#include <faabric/scheduler/MpiMessageBuffer.h>
#include <faabric/transport/MpiMessageEndpoint.h>
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

namespace faabric::scheduler {

// -----------------------------------
// Mocking
// -----------------------------------
// MPITOPTP - mocking at the MPI level won't be needed when using the PTP broker
// as the broker already has mocking capabilities
std::vector<faabric::MpiHostsToRanksMessage> getMpiHostsToRanksMessages();

std::vector<std::shared_ptr<faabric::MPIMessage>> getMpiMockedMessages(
  int sendRank);

typedef faabric::util::Queue<std::shared_ptr<faabric::MPIMessage>>
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

    int getId();

    int getSize();

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
              faabric::MPIMessage::MPIMessageType messageType =
                faabric::MPIMessage::NORMAL);

    int isend(int sendRank,
              int recvRank,
              const uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              faabric::MPIMessage::MPIMessageType messageType =
                faabric::MPIMessage::NORMAL);

    void broadcast(int rootRank,
                   int thisRank,
                   uint8_t* buffer,
                   faabric_datatype_t* dataType,
                   int count,
                   faabric::MPIMessage::MPIMessageType messageType =
                     faabric::MPIMessage::NORMAL);

    void recv(int sendRank,
              int recvRank,
              uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              MPI_Status* status,
              faabric::MPIMessage::MPIMessageType messageType =
                faabric::MPIMessage::NORMAL);

    int irecv(int sendRank,
              int recvRank,
              uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              faabric::MPIMessage::MPIMessageType messageType =
                faabric::MPIMessage::NORMAL);

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

  private:
    int id = -1;
    int size = -1;
    std::string thisHost;
    int basePort;
    faabric::util::TimePoint creationTime;

    std::atomic_flag isDestroyed = false;

    std::string user;
    std::string function;

    std::vector<int> cartProcsPerDim;

    /* MPI internal messaging layer */

    // Track at which host each rank lives
    std::vector<std::string> hostForRank;
    int getIndexForRanks(int sendRank, int recvRank);

    // Store the ranks that live in each host
    std::map<std::string, std::vector<int>> ranksForHost;

    // Track local and remote leaders. The leader is stored in the first
    // position of the host to ranks map.
    // MPITOPTP - this information exists in the broker
    int localLeader = -1;
    void initLocalRemoteLeaders();

    // In-memory queues for local messaging
    std::vector<std::shared_ptr<InMemoryMpiQueue>> localQueues;
    void initLocalQueues();

    // Rank-to-rank sockets for remote messaging
    std::vector<int> basePorts;
    std::vector<int> initLocalBasePorts(
      const std::vector<std::string>& executedAt);

    void initRemoteMpiEndpoint(int localRank, int remoteRank);

    std::pair<int, int> getPortForRanks(int localRank, int remoteRank);

    void sendRemoteMpiMessage(int sendRank,
                              int recvRank,
                              const std::shared_ptr<faabric::MPIMessage>& msg);

    std::shared_ptr<faabric::MPIMessage> recvRemoteMpiMessage(int sendRank,
                                                              int recvRank);

    faabric::MpiHostsToRanksMessage recvMpiHostRankMsg();

    void sendMpiHostRankMsg(const std::string& hostIn,
                            const faabric::MpiHostsToRanksMessage msg);

    void closeMpiMessageEndpoints();

    // Support for asyncrhonous communications
    std::shared_ptr<MpiMessageBuffer> getUnackedMessageBuffer(int sendRank,
                                                              int recvRank);

    std::shared_ptr<faabric::MPIMessage> recvBatchReturnLast(int sendRank,
                                                             int recvRank,
                                                             int batchSize = 0);

    /* Helper methods */

    void checkRanksRange(int sendRank, int recvRank);

    // Abstraction of the bulk of the recv work, shared among various functions
    void doRecv(std::shared_ptr<faabric::MPIMessage> m,
                uint8_t* buffer,
                faabric_datatype_t* dataType,
                int count,
                MPI_Status* status,
                faabric::MPIMessage::MPIMessageType messageType =
                  faabric::MPIMessage::NORMAL);
};
}
