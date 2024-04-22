#pragma once

#include <faabric/mpi/MpiMessage.h>
#include <faabric/mpi/mpi.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#include <atomic>

// Constants for profiling MPI parameters like number of messages sent or
// message breakdown by type in the execution graph. Remember to increase the
// counter if you add another one
#define NUM_MPI_EXEC_GRAPH_DETAILS 2
#define MPI_MSG_COUNT_PREFIX "mpi-msgcount-torank"
#define MPI_MSGTYPE_COUNT_PREFIX "mpi-msgtype-torank"

namespace faabric::mpi {

// -----------------------------------
// Mocking
// -----------------------------------
// MPITOPTP - mocking at the MPI level won't be needed when using the PTP broker
// as the broker already has mocking capabilities
std::vector<MpiMessage> getMpiMockedMessages(int sendRank);

#ifdef FAABRIC_USE_SPINLOCK
typedef faabric::util::SpinLockQueue<MpiMessage> InMemoryMpiQueue;
#else
typedef faabric::util::FixedCapacityQueue<MpiMessage> InMemoryMpiQueue;
#endif

class MpiWorld
{
  public:
    MpiWorld();

    void create(faabric::Message& call, int newId, int newSize);

    void broadcastHostsToRanks();

    // Initialise per-node world state, called once per world
    void initialiseFromMsg(faabric::Message& msg);

    // Initialise per rank (thread-local) state, called once per rank
    void initialiseRankFromMsg(faabric::Message& msg);

    std::string getHostForRank(int rank);

    std::string getUser();

    std::string getFunction();

    int getId() const;

    int getSize() const;

    // Returns true if the world is empty in this host and can be cleared from
    // the registry
    bool destroy();

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
              MpiMessageType messageType = MpiMessageType::NORMAL);

    int isend(int sendRank,
              int recvRank,
              const uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              MpiMessageType messageType = MpiMessageType::NORMAL);

    void broadcast(int rootRank,
                   int thisRank,
                   uint8_t* buffer,
                   faabric_datatype_t* dataType,
                   int count,
                   MpiMessageType messageType = MpiMessageType::NORMAL);

    void recv(int sendRank,
              int recvRank,
              uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              MPI_Status* status,
              MpiMessageType messageType = MpiMessageType::NORMAL);

    int irecv(int sendRank,
              int recvRank,
              uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              MpiMessageType messageType = MpiMessageType::NORMAL);

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

    /* Profiling */

    /* Function Migration */

    void prepareMigration(int newGroupId,
                          int thisRank,
                          bool thisRankMustMigrate);

  private:
    int id = -1;
    int size = -1;
    std::string thisHost;
    faabric::util::TimePoint creationTime;

    // Latch used to clear the world from the registry when we are migrating
    // out of it (i.e. evicting it). Note that this clean-up is only necessary
    // for migration, as we want to clean things up in case we ever migrate
    // again back into this host
    std::atomic<int> evictionLatch = 0;

    std::atomic_flag isDestroyed = false;

    std::string user;
    std::string function;

    std::vector<int> cartProcsPerDim;

    /* MPI internal messaging layer */

    // Track at which host each rank lives
    int getIndexForRanks(int sendRank, int recvRank) const;

    // Store the ranks that live in each host and host for each rank
    // WARN: it is important that ranksForHost only uses sorted collections,
    // as some collective communication algorithms rely on it being read in
    // the same order in all worlds
    std::map<std::string, std::set<int>> ranksForHost;
    std::vector<std::string> hostForRank;
    // For each remote rank, store the port that it is listening to
    std::vector<int> portForRank;

    // Track local and remote leaders. The leader is stored in the first
    // position of the host to ranks map.
    int localLeader = -1;
    void initLocalRemoteLeaders();

    // In-memory queues for local messaging
    std::vector<std::shared_ptr<InMemoryMpiQueue>> localQueues;
    void initLocalQueues();

    // Remote messaging using the PTP layer for control-plane
    faabric::transport::PointToPointBroker& broker;

    // TCP sockets for remote messaging
    void initSendRecvSockets();

    int getPortForRank(int rank);

    void sendRemoteMpiMessage(int sendRank, int recvRank, MpiMessage& msg);

    MpiMessage recvRemoteMpiMessage(int sendRank, int recvRank);

    // Support for asyncrhonous communications
    int getUnackedMessageBuffer(int sendRank, int recvRank);

    MpiMessage recvBatchReturnLast(int sendRank,
                                   int recvRank,
                                   int requestId = 0);

    /* Helper methods */

    void checkRanksRange(int sendRank, int recvRank);

    MpiMessage internalRecv(int sendRank, int recvRank, bool isLocal);

    // Abstraction of the bulk of the recv work, shared among various functions
    void doRecv(const MpiMessage& msg,
                uint8_t* buffer,
                faabric_datatype_t* dataType,
                int count,
                MPI_Status* status,
                MpiMessageType messageType = MpiMessageType::NORMAL);
};
}
