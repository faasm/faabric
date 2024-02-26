#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/mpi/MpiMessage.h>
#include <faabric/mpi/MpiWorld.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/macros.h>
#include <faabric/util/ExecGraph.h>
#include <faabric/util/batch.h>
#include <faabric/util/environment.h>
#include <faabric/util/gids.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/testing.h>

#include <list>
#include <map>

namespace faabric::mpi {

// -----------------------------------
// Mocking
// -----------------------------------
static std::mutex mockMutex;

// The identifier in this map is the sending rank. For the receiver's rank
// we can inspect the MpiMessage object
static std::map<int, std::vector<MpiMessage>> mpiMockedMessages;

std::vector<MpiMessage> getMpiMockedMessages(int sendRank)
{
    faabric::util::UniqueLock lock(mockMutex);
    return mpiMockedMessages[sendRank];
}

static const MpiMessage MPI_SHUTDOWN_MESSAGE = { .messageType =
                                                   SHUTDOWN_WORLD };

// -----------------------------------
// Thread-Local State
// Each MPI rank runs in a separate thread, thus we use TLS to maintain the
// per-rank data structures
// -----------------------------------

struct MpiRankState
{
    int msgCount = 1;

    // Message that created this thread-local instance
    faabric::Message* msg = nullptr;

    // This data structure contains one list for each <send, recv> pair
    // containing all the MPI messages for which we have posted an `irecv` but
    // have not `wait`ed on yet.
    std::vector<std::unique_ptr<std::list<MpiMessage>>> unackedMessageBuffers;

    // ----- Remote Messaging -----

    // This structure contains one send socket per remote rank
    std::vector<std::unique_ptr<faabric::transport::AsyncSendMessageEndpoint>>
      sendSockets;

    // This method and background thread are responsible to receiving messages
    // from the network for this rank
    std::unique_ptr<std::jthread> recvThread;
    std::stop_source stopSource;

    void reset()
    {
        // Unacked message buffers
        if (!unackedMessageBuffers.empty()) {
            for (auto& umb : unackedMessageBuffers) {
                if (umb != nullptr) {
                    if (!umb->empty()) {
                        SPDLOG_ERROR(
                          "Destroying the MPI world with outstanding {}"
                          " messages in the message buffer",
                          umb->size());
                        throw std::runtime_error(
                          "Destroying world with a non-empty MPI message "
                          "buffer");
                    }
                }
            }
            unackedMessageBuffers.clear();
        }

        // Send sockets (recv thread is cleared as part of separate call)
        sendSockets.clear();

        // Local message count
        msgCount = 1;

        // Rank message
        msg = nullptr;
    }
};

static thread_local MpiRankState rankState;

MpiWorld::MpiWorld()
  : thisHost(faabric::util::getSystemConfig().endpointHost)
  , creationTime(faabric::util::startTimer())
  , cartProcsPerDim(2)
  , broker(faabric::transport::getPointToPointBroker())
{}

int MpiWorld::getSendSocket(int sendRank, int recvRank)
{
    // We want to lazily initialise this data structure because, given its
    // thread local nature, we expect it to be quite sparse (i.e. filled with
    // nullptr).
    if (rankState.sendSockets.empty()) {
        rankState.sendSockets = std::vector<
          std::unique_ptr<faabric::transport::AsyncSendMessageEndpoint>>(size);
    }

    int index = recvRank;
    std::string dstHost = getHostForRank(recvRank);
    int dstPort = getPortForRank(recvRank);
    if (rankState.sendSockets[index] == nullptr) {
        rankState.sendSockets.at(index) =
          std::make_unique<faabric::transport::AsyncSendMessageEndpoint>(
            dstHost, dstPort);
    }

    return index;
}

void MpiWorld::sendRemoteMpiMessage(std::string dstHost,
                                    int sendRank,
                                    int recvRank,
                                    const MpiMessage& msg)
{
    // Serialise
    // TODO(mpi-sock): expose a sendv like interface and avoid serialising here
    std::vector<uint8_t> serialisedBuffer(msgSize(msg));
    serializeMpiMsg(serialisedBuffer, msg);

    int index = getSendSocket(sendRank, recvRank);

    try {
        // TODO(mpi-sock): as we are bypassing the PTP layer we can null-out
        // the header. We should consider using an MPI-only wire format
        rankState.sendSockets.at(index)->send(
          0,
          reinterpret_cast<const uint8_t*>(serialisedBuffer.data()),
          serialisedBuffer.size());
    } catch (std::runtime_error& e) {
        SPDLOG_ERROR("{}:{}:{} Timed out with: MPI - send {} -> {}",
                     rankState.msg->appid(),
                     rankState.msg->groupid(),
                     rankState.msg->groupidx(),
                     sendRank,
                     recvRank);
        throw e;
    }
}

// This method lazily initialises the unacked message buffer's and returns
// an index to the one we want
int MpiWorld::getUnackedMessageBuffer(int sendRank, int recvRank)
{
    // We want to lazily initialise this data structure because, given its
    // thread local nature, we expect it to be quite sparse (i.e. filled with
    // nullptr).
    if (rankState.unackedMessageBuffers.empty()) {
        rankState.unackedMessageBuffers.clear();
        rankState.unackedMessageBuffers =
          std::vector<std::unique_ptr<std::list<MpiMessage>>>(size * size);
    }

    // Get the index for the rank-host pair
    int index = getIndexForRanks(sendRank, recvRank);
    assert(index >= 0 && index < size * size);

    if (rankState.unackedMessageBuffers[index] == nullptr) {
        rankState.unackedMessageBuffers.at(index) =
          std::make_unique<std::list<MpiMessage>>();
    }

    return index;
}

void MpiWorld::create(faabric::Message& call, int newId, int newSize)
{
    id = newId;
    user = call.user();
    function = call.function();
    rankState.msg = &call;
    size = newSize;

    // Update the first message to make sure it looks like messages >= 1
    call.set_ismpi(true);
    call.set_mpirank(0);
    call.set_mpiworldid(id);
    call.set_mpiworldsize(size);
    call.set_groupidx(call.mpirank());
    call.set_appidx(call.mpirank());

    // Dispatch all the chained calls. With the main being rank zero, we want
    // to spawn (size - 1) new functions starting with rank 1
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory(user, function, size - 1);
    faabric::util::updateBatchExecAppId(req, call.appid());
    for (int i = 0; i < req->messages_size(); i++) {
        // Update MPI-related fields
        faabric::Message& msg = req->mutable_messages()->at(i);
        msg.set_ismpi(true);
        msg.set_mpiworldid(call.mpiworldid());
        msg.set_mpirank(i + 1);
        msg.set_mpiworldsize(call.mpiworldsize());

        // Set group idxs for remote messaging
        msg.set_groupidx(msg.mpirank());
        if (rankState.msg != nullptr) {
            // Set message fields to allow for function migration
            msg.set_appid(rankState.msg->appid());
            msg.set_cmdline(rankState.msg->cmdline());
            msg.set_inputdata(rankState.msg->inputdata());

            // Log chained functions to generate execution graphs
            if (rankState.msg->recordexecgraph()) {
                faabric::util::logChainedFunction(call, msg);
                msg.set_recordexecgraph(true);
            }
        }
    }

    // As a result of the call to the scheduler, a point-to-point communcation
    // group will have been created. We update our recorded message group id
    // to use the new PTP group
    if (size > 1) {
        auto decision = faabric::planner::getPlannerClient().callFunctions(req);
        rankState.msg->set_groupid(decision.groupId);
        assert(decision.hosts.size() == size - 1);
    } else {
        // If world has size one, create the communication group (of size one)
        // manually.
        faabric::batch_scheduler::SchedulingDecision decision(id, id);
        call.set_groupidx(0);
        decision.addMessage(thisHost, call);
        broker.setAndSendMappingsFromSchedulingDecision(decision);
    }

    // Record which ranks are local to this world, and query for all leaders
    initLocalRemoteLeaders();
    // Given that we are initialising the whole MpiWorld here, the local leader
    // should also be rank 0
    assert(localLeader == 0);

    // Initialise the memory queues for message reception
    initLocalQueues();

    // Finally, start the receiving thread for this rank
    initRecvThread();
}

void MpiWorld::destroy()
{
    SPDLOG_TRACE("Destroying MPI world {}", id);

    // ----- Per-rank cleanup -----

    // First, stop the thread-local network-polling thread
    stopRecvThread();

    // Now we can safely reset all other thread-local structures
    rankState.reset();

    // Clear structures used for mocking
    {
        faabric::util::UniqueLock lock(mockMutex);
        mpiMockedMessages.clear();
    }
}

void MpiWorld::initialiseFromMsg(faabric::Message& msg)
{
    id = msg.mpiworldid();
    user = msg.user();
    function = msg.function();
    size = msg.mpiworldsize();
    rankState.msg = &msg;

    // Record which ranks are local to this world, and query for all leaders
    initLocalRemoteLeaders();

    // Initialise the memory queues for message reception
    initLocalQueues();

    // Initialise the receiver thread
    initRecvThread();
}

void MpiWorld::setMsgForRank(faabric::Message& msg)
{
    rankState.msg = &msg;

    // Initialise the receiver thread for this rank
    initRecvThread();
}

std::string MpiWorld::getHostForRank(int rank)
{
    // This method may be called a lot, so we query our cached records instead
    // of the point-to-point broker, where we need to acquire a shared lock for
    // every query.
    return hostForRank.at(rank);
}

int MpiWorld::getPortForRank(int rank)
{
    return portForRank.at(rank);
}

// The local leader for an MPI world is defined as the lowest rank assigned to
// this host. For simplicity, we set the local leader to be the first element
// in the ranks to hosts map.
void MpiWorld::initLocalRemoteLeaders()
{
    // Clear the existing map in case we are calling this method during a
    // migration
    ranksForHost.clear();
    hostForRank.clear();
    portForRank.clear();

    // First, group the ranks per host they belong to for convinience. We also
    // keep a record of the opposite mapping, the host that each rank belongs
    // to, as it is queried frequently and asking the ptp broker involves
    // acquiring a lock.
    if (rankState.msg == nullptr) {
        throw std::runtime_error("Rank message not set!");
    }
    int groupId = rankState.msg->groupid();
    std::set<int> rankIds = broker.getIdxsRegisteredForGroup(groupId);
    if (rankIds.size() != size) {
        SPDLOG_ERROR("{}:{}:{} rankIds != size ({} != {})",
                     rankState.msg->appid(),
                     groupId,
                     rankState.msg->groupidx(),
                     rankIds.size(),
                     size);
        throw std::runtime_error("MPI Group-World size mismatch!");
    }
    assert(rankIds.size() == size);
    hostForRank.resize(size);
    portForRank.resize(size);
    for (const auto& rankId : rankIds) {
        std::string host = broker.getHostForReceiver(groupId, rankId);
        ranksForHost[host].insert(rankId);
        hostForRank.at(rankId) = host;
        portForRank.at(rankId) = broker.getMpiPortForReceiver(groupId, rankId);
    }

    // Add the port for this rank
    int thisRank = rankState.msg->groupidx();
    portForRank.at(thisRank) = broker.getMpiPortForReceiver(groupId, thisRank);

    // Persist the local leader in this host for further use
    localLeader = (*ranksForHost[thisHost].begin());
}

void MpiWorld::getCartesianRank(int rank,
                                int maxDims,
                                const int* dims,
                                int* periods,
                                int* coords)
{
    if (rank > this->size - 1) {
        throw std::runtime_error(
          fmt::format("Rank {} bigger than world size {}", rank, this->size));
    }
    // Pre-requisite: dims[0] * dims[1] == nprocs
    // Note: we don't support 3-dim grids
    if ((dims[0] * dims[1]) != this->size) {
        throw std::runtime_error(
          fmt::format("Product of ranks across dimensions not equal to world "
                      "size, {} x {} != {}",
                      dims[0],
                      dims[1],
                      this->size));
    }

    // Store the cartesian dimensions for further use. All ranks have the same
    // vector.
    // Note that we could only store one of the two, and derive the other
    // from the world size.
    this->cartProcsPerDim[0] = dims[0];
    this->cartProcsPerDim[1] = dims[1];

    // Compute the coordinates in a 2-dim grid of the original process rank.
    // As input we have a vector containing the number of processes per
    // dimension (dims).
    // We have dims[0] x dims[1] = N slots, thus:
    coords[0] = rank / dims[1];
    coords[1] = rank % dims[1];

    // LAMMPS always uses periodic grids. So do we.
    periods[0] = 1;
    periods[1] = 1;

    // The remaining dimensions should be 1, and the coordinate of our rank 0
    for (int i = 2; i < maxDims; i++) {
        if (dims[i] != 1) {
            throw std::runtime_error(
              fmt::format("Non-zero number of processes in dimension greater "
                          "than 2. {} -> {}",
                          i,
                          dims[i]));
        }
        coords[i] = 0;
        periods[i] = 1;
    }
}

void MpiWorld::getRankFromCoords(int* rank, int* coords)
{
    // Note that we only support 2 dim grids. In each dimension we have
    // cartProcsPerDim[0] and cartProcsPerDim[1] processes respectively.

    // Pre-requisite: cartProcsPerDim[0] * cartProcsPerDim[1] == nprocs
    if ((this->cartProcsPerDim[0] * this->cartProcsPerDim[1]) != this->size) {
        throw std::runtime_error(fmt::format(
          "Processors per dimension don't match world size: {} x {} != {}",
          this->cartProcsPerDim[0],
          this->cartProcsPerDim[1],
          this->size));
    }

    // This is the inverse of finding the coordinates for a rank
    *rank = coords[1] + coords[0] * cartProcsPerDim[1];
}

void MpiWorld::shiftCartesianCoords(int rank,
                                    int direction,
                                    int disp,
                                    int* source,
                                    int* destination)
{
    // rank: is the process the method is being called from (i.e. me)
    // source: the rank that reaches me moving <disp> units in <direction>
    // destination: is the rank I reach moving <disp> units in <direction>

    // Get the coordinates for my rank
    std::vector<int> coords = { rank / cartProcsPerDim[1],
                                rank % cartProcsPerDim[1] };

    // Move <disp> units in <direction> forward with periodicity
    // Note: we always use periodicity and 2 dimensions because LAMMMPS does.
    std::vector<int> dispCoordsFwd;
    if (direction == 0) {
        dispCoordsFwd = { (coords[0] + disp) % cartProcsPerDim[0], coords[1] };
    } else if (direction == 1) {
        dispCoordsFwd = { coords[0], (coords[1] + disp) % cartProcsPerDim[1] };
    } else {
        dispCoordsFwd = { coords[0], coords[1] };
    }
    // If direction >=2 we are in a dimension we don't use, hence we are the
    // only process, and we always land in our coordinates (due to periodicity)

    // Fill the destination variable
    getRankFromCoords(destination, dispCoordsFwd.data());

    // Move <disp> units in <direction> backwards with periodicity
    // Note: as subtracting may yield a negative result, we add a full loop
    // to prevent taking the modulo of a negative value.
    std::vector<int> dispCoordsBwd;
    if (direction == 0) {
        dispCoordsBwd = { (coords[0] - disp + cartProcsPerDim[0]) %
                            cartProcsPerDim[0],
                          coords[1] };
    } else if (direction == 1) {
        dispCoordsBwd = { coords[0],
                          (coords[1] - disp + cartProcsPerDim[1]) %
                            cartProcsPerDim[1] };
    } else {
        dispCoordsBwd = { coords[0], coords[1] };
    }

    // Fill the source variable
    getRankFromCoords(source, dispCoordsBwd.data());
}

const uint8_t iSendMagic = 0xFF;
const uint8_t iRecvMagic = 0x00;

// Helper method to generate an async. request id. We encode the ranks, a
// unique id, and wether it is an isend or an irecv request on an int32_t
// as follows:
// - First byte corresponds to isend (
static int32_t getAsyncRequestId(int sendRank, int recvRank, bool isSend)
{
    // WARNING: this method assumes that all ranks actually fit in a single
    // byte
    assert(sendRank < 256);
    assert(recvRank < 256);

    // We operate on an unsigned integer to avoid undefined behaviour
    uint32_t requestId = 0;

    // First add the magic in the leftmost byte
    if (isSend) {
        requestId |= (iSendMagic << 24);
    } else {
        requestId |= (iRecvMagic << 24);
    }

    // Second, add a unique id for the async request. Note that we never have
    // hundreds of outstanding requests for the same (send, recv) pair
    uint8_t uniqueId = (faabric::util::generateGid() & 0xFF);
    requestId |= (uniqueId << 16);

    // Third add the send rank
    requestId |= ((sendRank & 0xFF) << 8);

    // Lastly, add the recv rank
    requestId |= (recvRank & 0xFF);

    return (int32_t)requestId;
}

static std::tuple<int, int, bool> getRanksFromRequestId(int32_t requestId)
{
    uint32_t reqId = (uint32_t)requestId;

    int32_t recvRank = reqId & 0xFF;
    int32_t sendRank = (reqId >> 8) & 0xFF;

    bool isSend = ((reqId >> 24) & 0xFF) == iSendMagic;

    return { sendRank, recvRank, isSend };
}

// Sending is already asynchronous in both transport layers we use: in-memory
// queues for local messages, and TCP sockets for remote messages. Thus,
// we can just send normally and return a requestId. Upon await, we'll return
// immediately.
int MpiWorld::isend(int sendRank,
                    int recvRank,
                    const uint8_t* buffer,
                    faabric_datatype_t* dataType,
                    int count,
                    MpiMessageType messageType)
{
    // int requestId = (int)faabric::util::generateGid();
    // iSendRequests.insert(requestId);
    int32_t requestId = getAsyncRequestId(sendRank, recvRank, true);

    send(sendRank, recvRank, buffer, dataType, count, messageType);

    return requestId;
}

int MpiWorld::irecv(int sendRank,
                    int recvRank,
                    uint8_t* buffer,
                    faabric_datatype_t* dataType,
                    int count,
                    MpiMessageType messageType)
{
    int32_t requestId = getAsyncRequestId(sendRank, recvRank, false);

    SPDLOG_TRACE(
      "MPI - irecv {} -> {} (reqId: {})", sendRank, recvRank, requestId);

    // Note that when we call `irecv` we can use the provided buffer until
    // we call wait, so we do not need to malloc memory here
    MpiMessage msg = { .id = 0,
                       .worldId = id,
                       .sendRank = sendRank,
                       .recvRank = recvRank,
                       .typeSize = dataType->size,
                       .count = count,
                       .requestId = requestId,
                       .messageType = MpiMessageType::UNACKED_MPI_MESSAGE,
                       .buffer = (void*)buffer };

    auto index = getUnackedMessageBuffer(sendRank, recvRank);
    rankState.unackedMessageBuffers.at(index)->push_back(msg);

    return requestId;
}

void MpiWorld::send(int sendRank,
                    int recvRank,
                    const uint8_t* buffer,
                    faabric_datatype_t* dataType,
                    int count,
                    MpiMessageType messageType)
{
    // Sanity-check input parameters
    checkRanksRange(sendRank, recvRank);
    if (getHostForRank(sendRank) != thisHost) {
        SPDLOG_ERROR("Trying to send message from a non-local rank: {}",
                     sendRank);
        throw std::runtime_error("Sending message from non-local rank");
    }

    // Work out whether the message is sent locally or to another host
    const std::string otherHost = getHostForRank(recvRank);
    bool isLocal = otherHost == thisHost;

    // Generate a message ID
    int msgId = (rankState.msgCount + 1) % INT32_MAX;

    MpiMessage msg = { .id = msgId,
                       .worldId = id,
                       .sendRank = sendRank,
                       .recvRank = recvRank,
                       .typeSize = dataType->size,
                       .count = count,
                       .messageType = messageType,
                       .buffer = nullptr };

    // Mock the message sending in tests
    if (faabric::util::isMockMode()) {
        mpiMockedMessages[sendRank].push_back(msg);
        return;
    }

    bool mustSendData = count > 0 && buffer != nullptr;

    // Dispatch the message locally or globally
    if (isLocal) {
        // Take control over the buffer data if we are gonna move it to
        // the in-memory queues for local messaging
        if (mustSendData) {
            void* bufferPtr = faabric::util::malloc(count * dataType->size);
            std::memcpy(bufferPtr, buffer, count * dataType->size);

            msg.buffer = bufferPtr;
        }

        SPDLOG_TRACE(
          "MPI - send {} -> {} ({})", sendRank, recvRank, messageType);
        getLocalQueue(sendRank, recvRank)->enqueue(msg);
    } else {
        if (mustSendData) {
            msg.buffer = (void*)buffer;
        }

        SPDLOG_TRACE(
          "MPI - send remote {} -> {} ({})", sendRank, recvRank, messageType);
        sendRemoteMpiMessage(otherHost, sendRank, recvRank, msg);
    }

    /* 02/05/2022 - The following bit of code fails randomly with a protobuf
     * assertion error
    // If the message is set and recording on, track we have sent this message
    if (thisRankMsg != nullptr && thisRankMsg->recordexecgraph()) {
        faabric::util::exec_graph::incrementCounter(
          *thisRankMsg,
          fmt::format("{}-{}", MPI_MSG_COUNT_PREFIX, std::to_string(recvRank)));

        // Work out the message type breakdown
        faabric::util::exec_graph::incrementCounter(
          *thisRankMsg,
          fmt::format("{}-{}-{}",
                      MPI_MSGTYPE_COUNT_PREFIX,
                      std::to_string(messageType),
                      std::to_string(recvRank)));
    }
    */
}

void MpiWorld::recv(int sendRank,
                    int recvRank,
                    uint8_t* buffer,
                    faabric_datatype_t* dataType,
                    int count,
                    MPI_Status* status,
                    MpiMessageType messageType)
{
    // Sanity-check input parameters
    checkRanksRange(sendRank, recvRank);

    // If mocking the messages, ignore calls to receive that may block
    if (faabric::util::isMockMode()) {
        return;
    }

    // Given that we share the same in-memory queues for async and non-async
    // messages, when we call MPI_Recv and dequeue from the queue, we will
    // first receive all the messages that we have MPI_Irecv-ed first

    auto msg = recvBatchReturnLast(sendRank, recvRank);

    doRecv(std::move(msg), buffer, dataType, count, status, messageType);
}

void MpiWorld::doRecv(const MpiMessage& m,
                      uint8_t* buffer,
                      faabric_datatype_t* dataType,
                      int count,
                      MPI_Status* status,
                      MpiMessageType messageType)
{
    // Assert message integrity
    // Note - this checks won't happen in Release builds
    if (m.messageType != messageType) {
        SPDLOG_ERROR("Different message types (got: {}, expected: {})",
                     m.messageType,
                     messageType);
    }
    assert(m.messageType == messageType);
    assert(m.count <= count);

    // We must copy the data into the application-provided buffer
    if (m.count > 0 && m.buffer != nullptr) {
        // Make sure we do not overflow the recepient buffer
        auto bytesToCopy =
          std::min<size_t>(m.count * dataType->size, count * dataType->size);
        std::memcpy(buffer, m.buffer, bytesToCopy);

        // This buffer has been malloc-ed either as part of a local `send`
        // or as part of a remote `parseMpiMsg`
        faabric::util::free((void*)m.buffer);
    }

    // Set status values if required
    if (status != nullptr) {
        status->MPI_SOURCE = m.sendRank;
        status->MPI_ERROR = MPI_SUCCESS;

        // Take the message size here as the receive count may be larger
        status->bytesSize = m.count * dataType->size;

        // TODO - thread through tag
        status->MPI_TAG = -1;
    }
}

void MpiWorld::sendRecv(uint8_t* sendBuffer,
                        int sendCount,
                        faabric_datatype_t* sendDataType,
                        int sendRank,
                        uint8_t* recvBuffer,
                        int recvCount,
                        faabric_datatype_t* recvDataType,
                        int recvRank,
                        int myRank,
                        MPI_Status* status)
{
    SPDLOG_TRACE("MPI - Sendrecv. Rank {}. Sending to: {} - Receiving from: {}",
                 myRank,
                 sendRank,
                 recvRank);

    // Post async recv
    int recvId = irecv(recvRank,
                       myRank,
                       recvBuffer,
                       recvDataType,
                       recvCount,
                       MpiMessageType::SENDRECV);
    // Then send the message
    send(myRank,
         sendRank,
         sendBuffer,
         sendDataType,
         sendCount,
         MpiMessageType::SENDRECV);
    // And wait
    awaitAsyncRequest(recvId);
}

void MpiWorld::broadcast(int sendRank,
                         int recvRank,
                         uint8_t* buffer,
                         faabric_datatype_t* dataType,
                         int count,
                         MpiMessageType messageType)
{
    SPDLOG_TRACE("MPI - bcast {} -> {}", sendRank, recvRank);

    if (recvRank == sendRank) {
        for (auto it : ranksForHost) {
            if (it.first == thisHost) {
                // Send message to all our local ranks besides ourselves
                for (const int localRecvRank : it.second) {
                    if (localRecvRank == recvRank) {
                        continue;
                    }

                    send(recvRank,
                         localRecvRank,
                         buffer,
                         dataType,
                         count,
                         messageType);
                }
            } else {
                // Send message to the local leader of each remote host. Note
                // that the local leader will then broadcast the message to its
                // local ranks.
                send(recvRank,
                     *(it.second.begin()),
                     buffer,
                     dataType,
                     count,
                     messageType);
            }
        }
    } else if (recvRank == localLeader) {
        // If we are the local leader, first we receive the message sent by
        // the sending rank
        recv(sendRank, recvRank, buffer, dataType, count, nullptr, messageType);

        // If the broadcast originated locally, we are done. If not, we now
        // distribute to all our local ranks
        if (getHostForRank(sendRank) != thisHost) {
            for (const int localRecvRank : ranksForHost[thisHost]) {
                if (localRecvRank == recvRank) {
                    continue;
                }

                send(recvRank,
                     localRecvRank,
                     buffer,
                     dataType,
                     count,
                     messageType);
            }
        }
    } else {
        // If we are neither the sending rank nor a local leader, we receive
        // from either our local leader if the broadcast originated in a
        // different host, or the sending rank itself if we are on the same host
        int sendingRank =
          getHostForRank(sendRank) == thisHost ? sendRank : localLeader;

        recv(
          sendingRank, recvRank, buffer, dataType, count, nullptr, messageType);
    }
}

void checkSendRecvMatch(faabric_datatype_t* sendType,
                        int sendCount,
                        faabric_datatype_t* recvType,
                        int recvCount)
{
    if (sendType->id != recvType->id && sendCount == recvCount) {
        SPDLOG_ERROR("Must match type/ count (send {}:{}, recv {}:{})",
                     sendType->id,
                     sendCount,
                     recvType->id,
                     recvCount);
        throw std::runtime_error("Mismatching send/ recv");
    }
}

void MpiWorld::scatter(int sendRank,
                       int recvRank,
                       const uint8_t* sendBuffer,
                       faabric_datatype_t* sendType,
                       int sendCount,
                       uint8_t* recvBuffer,
                       faabric_datatype_t* recvType,
                       int recvCount)
{
    checkSendRecvMatch(sendType, sendCount, recvType, recvCount);

    size_t sendOffset = sendCount * sendType->size;

    // If we're the sender, do the sending
    if (recvRank == sendRank) {
        SPDLOG_TRACE("MPI - scatter {} -> all", sendRank);

        for (int r = 0; r < size; r++) {
            // Work out the chunk of the send buffer to send to this rank
            const uint8_t* startPtr = sendBuffer + (r * sendOffset);

            if (r == sendRank) {
                // Copy data directly if this is the send rank
                const uint8_t* endPtr = startPtr + sendOffset;
                std::copy(startPtr, endPtr, recvBuffer);
            } else {
                send(sendRank,
                     r,
                     startPtr,
                     sendType,
                     sendCount,
                     MpiMessageType::SCATTER);
            }
        }
    } else {
        // Do the receiving
        recv(sendRank,
             recvRank,
             recvBuffer,
             recvType,
             recvCount,
             nullptr,
             MpiMessageType::SCATTER);
    }
}

void MpiWorld::gather(int sendRank,
                      int recvRank,
                      const uint8_t* sendBuffer,
                      faabric_datatype_t* sendType,
                      int sendCount,
                      uint8_t* recvBuffer,
                      faabric_datatype_t* recvType,
                      int recvCount)
{
    checkSendRecvMatch(sendType, sendCount, recvType, recvCount);
    size_t sendSize = sendCount * sendType->size;
    size_t recvSize = recvCount * recvType->size;

    // This method does a two-step gather where each local leader does a gather
    // for its local ranks, and then the receiver and the local leaders do
    // one global gather. There are five scenarios:
    // 1. The rank calling gather is the receiver of the gather. This rank
    //    expects all its local ranks and the remote local leaders to send their
    //    data for gathering.
    // 2. The rank calling gather is a local leader, not co-located with the
    //    gather receiver. This rank expects all its local ranks to send their
    //    data for gathering, and then sends the resulting aggregation to the
    //    gather receiver.
    // 3. The rank calling gather is a local leader, co-located with the gather
    //    receiver. This rank just sends its data for gathering to the gather
    //    receiver.
    // 4. The rank calling gather is not a local leader, not co-located with
    //    the gather receiver. This rank sends its data for gathering to its
    //    local leader.
    // 5. The rank calling gather is a not a local leader, co-located with the
    //    gather receiver. This rank sends its data for gathering to the gather
    //    receiver.

    bool isGatherReceiver = sendRank == recvRank;
    bool isLocalLeader = sendRank == localLeader;
    bool isLocalGather = getHostForRank(recvRank) == thisHost;

    // Additionally, when sending data from gathering we must also differentiate
    // between two scenarios.
    // 1. Sending rank sets the MPI_IN_PLACE flag. This means the gather is part
    // of an allGather, and the sending rank has allocated enough space for all
    // ranks in the sending buffer. As a consequence, the to-be-gathered data
    // is in the offset corresponding to the sending rank.
    // 2. Sending rank does not set the MPI_IN_PLACE flag. This means that the
    // sending buffer only contains the to-be-gathered data.

    bool isInPlace = sendBuffer == recvBuffer;
    size_t sendBufferOffset = isInPlace ? sendRank * sendSize : 0;

    if (isGatherReceiver) {
        // Scenario 1
        SPDLOG_TRACE("MPI - gather all -> {}", recvRank);

        for (auto it : ranksForHost) {
            if (it.first == thisHost) {
                // Receive from all local ranks besides ourselves
                for (const int r : it.second) {
                    // If receiving from ourselves, but not in place, copy our
                    // data to the right offset
                    if (r == recvRank && !isInPlace) {
                        ::memcpy(recvBuffer + (recvRank * recvSize),
                                 sendBuffer,
                                 sendSize);
                    } else if (r != recvRank) {
                        recv(r,
                             recvRank,
                             recvBuffer + (r * recvSize),
                             recvType,
                             recvCount,
                             nullptr,
                             MpiMessageType::GATHER);
                    }
                }
            } else {
                // Receive from remote local leaders their local gathered data
                auto rankData =
                  std::make_unique<uint8_t[]>(it.second.size() * recvSize);

                recv(*(it.second.begin()),
                     recvRank,
                     rankData.get(),
                     recvType,
                     recvCount * it.second.size(),
                     nullptr,
                     MpiMessageType::GATHER);

                // Copy each received chunk to its offset
                auto itr = it.second.begin();
                for (int r = 0; r < it.second.size(); r++, itr++) {
                    ::memcpy(recvBuffer + ((*itr) * recvSize),
                             rankData.get() + (r * recvSize),
                             recvSize);
                }
            }
        }
    } else if (isLocalLeader && !isLocalGather) {
        // Scenario 2
        auto rankData =
          std::make_unique<uint8_t[]>(ranksForHost[thisHost].size() * sendSize);

        // Gather all our local ranks data and send in a single remote message
        auto localItr = ranksForHost[thisHost].begin();
        for (int r = 0; r < ranksForHost[thisHost].size(); r++, localItr++) {
            if ((*localItr) == sendRank) {
                // Receive from ourselves, just copy from/to the right offset
                ::memcpy(rankData.get() + r * sendSize,
                         sendBuffer + sendBufferOffset,
                         sendSize);
            } else {
                // Receive from other local ranks
                recv((*localItr),
                     sendRank,
                     rankData.get() + r * sendSize,
                     sendType,
                     sendCount,
                     nullptr,
                     MpiMessageType::GATHER);
            }
        }

        // Send the locally-gathered data to the receiver rank
        send(sendRank,
             recvRank,
             rankData.get(),
             sendType,
             sendCount * ranksForHost[thisHost].size(),
             MpiMessageType::GATHER);

    } else if (isLocalLeader && isLocalGather) {
        // Scenario 3
        send(sendRank,
             recvRank,
             sendBuffer + sendBufferOffset,
             sendType,
             sendCount,
             MpiMessageType::GATHER);
    } else if (!isLocalLeader && !isLocalGather) {
        // Scenario 4
        send(sendRank,
             localLeader,
             sendBuffer + sendBufferOffset,
             sendType,
             sendCount,
             MpiMessageType::GATHER);
    } else if (!isLocalLeader && isLocalGather) {
        // Scenario 5
        send(sendRank,
             recvRank,
             sendBuffer + sendBufferOffset,
             sendType,
             sendCount,
             MpiMessageType::GATHER);
    } else {
        SPDLOG_ERROR("Don't know how to gather rank's data.");
        SPDLOG_ERROR("- sendRank: {}\n- recvRank: {}\n- isGatherReceiver: "
                     "{}\n- isLocalLeader: {}\n- isLocalGather:{}",
                     sendRank,
                     recvRank,
                     isGatherReceiver,
                     isLocalLeader,
                     isLocalGather);
        throw std::runtime_error("Don't know how to gather rank's data.");
    }
}

void MpiWorld::allGather(int rank,
                         const uint8_t* sendBuffer,
                         faabric_datatype_t* sendType,
                         int sendCount,
                         uint8_t* recvBuffer,
                         faabric_datatype_t* recvType,
                         int recvCount)
{
    checkSendRecvMatch(sendType, sendCount, recvType, recvCount);

    int root = 0;

    // Do a gather with a hard-coded root
    gather(rank,
           root,
           sendBuffer,
           sendType,
           sendCount,
           recvBuffer,
           recvType,
           recvCount);

    // Note that sendCount and recvCount here are per-rank, so we need to work
    // out the full buffer size
    int fullCount = recvCount * size;

    // Do a broadcast with a hard-coded root
    broadcast(
      root, rank, recvBuffer, recvType, fullCount, MpiMessageType::ALLGATHER);
}

void MpiWorld::awaitAsyncRequest(int requestId)
{
    SPDLOG_TRACE("MPI - await {}", requestId);

    auto [sendRank, recvRank, isSend] = getRanksFromRequestId(requestId);

    if (isSend) {
        return;
    }

    // Indicate recvBatchReturnLast to stop when we find the given request id
    (void)recvBatchReturnLast(sendRank, recvRank, requestId);
}

void MpiWorld::reduce(int sendRank,
                      int recvRank,
                      uint8_t* sendBuffer,
                      uint8_t* recvBuffer,
                      faabric_datatype_t* datatype,
                      int count,
                      faabric_op_t* operation)
{
    size_t bufferSize = datatype->size * count;
    auto rankData = std::make_unique<uint8_t[]>(bufferSize);

    if (sendRank == recvRank) {
        SPDLOG_TRACE("MPI - reduce ({}) all -> {}", operation->id, recvRank);

        // If not receiving in-place, initialize the receive buffer to the send
        // buffer values. This prevents issues when 0-initializing for operators
        // like the minimum, or product.
        // If we're receiving from ourselves and in-place, our work is
        // already done and the results are written in the recv buffer
        bool isInPlace = sendBuffer == recvBuffer;
        if (!isInPlace) {
            ::memcpy(recvBuffer, sendBuffer, bufferSize);
        }

        for (auto it : ranksForHost) {
            if (it.first == thisHost) {
                // Reduce all data from our local ranks besides ourselves
                for (const int r : it.second) {
                    if (r == recvRank) {
                        continue;
                    }

                    memset(rankData.get(), 0, bufferSize);
                    recv(r,
                         recvRank,
                         rankData.get(),
                         datatype,
                         count,
                         nullptr,
                         MpiMessageType::REDUCE);

                    op_reduce(
                      operation, datatype, count, rankData.get(), recvBuffer);
                }
            } else {
                // For remote ranks, only receive from the host leader
                memset(rankData.get(), 0, bufferSize);
                recv((*it.second.begin()),
                     recvRank,
                     rankData.get(),
                     datatype,
                     count,
                     nullptr,
                     MpiMessageType::REDUCE);

                op_reduce(
                  operation, datatype, count, rankData.get(), recvBuffer);
            }
        }

    } else if (sendRank == localLeader) {
        // If we are the local leader (but not the receiver of the reduce) and
        // the receiver is not co-located with us, do a reduce with the data of
        // all our local ranks, and then send the result to the receiver
        if (getHostForRank(recvRank) != thisHost) {
            // In this step we reduce our local ranks data. It is important
            // that we do so in a copy of the send buffer, as the application
            // does not expect said buffer's contents to be modified.
            auto sendBufferCopy = std::make_unique<uint8_t[]>(bufferSize);
            ::memcpy(sendBufferCopy.get(), sendBuffer, bufferSize);

            for (const int r : ranksForHost[thisHost]) {
                if (r == sendRank) {
                    continue;
                }

                memset(rankData.get(), 0, bufferSize);
                recv(r,
                     sendRank,
                     rankData.get(),
                     datatype,
                     count,
                     nullptr,
                     MpiMessageType::REDUCE);

                op_reduce(operation,
                          datatype,
                          count,
                          rankData.get(),
                          sendBufferCopy.get());
            }

            send(sendRank,
                 recvRank,
                 sendBufferCopy.get(),
                 datatype,
                 count,
                 MpiMessageType::REDUCE);
        } else {
            // Send to the receiver rank
            send(sendRank,
                 recvRank,
                 sendBuffer,
                 datatype,
                 count,
                 MpiMessageType::REDUCE);
        }
    } else {
        // If we are neither the receiver of the reduce nor a local leader, we
        // send our data for reduction either to our local leader or the
        // receiver, depending on whether we are colocated with the receiver or
        // not
        int realRecvRank =
          getHostForRank(recvRank) == thisHost ? recvRank : localLeader;

        send(sendRank,
             realRecvRank,
             sendBuffer,
             datatype,
             count,
             MpiMessageType::REDUCE);
    }
}

void MpiWorld::allReduce(int rank,
                         uint8_t* sendBuffer,
                         uint8_t* recvBuffer,
                         faabric_datatype_t* datatype,
                         int count,
                         faabric_op_t* operation)
{
    // Rank 0 coordinates the allreduce operation
    // First, all ranks reduce to rank 0
    reduce(rank, 0, sendBuffer, recvBuffer, datatype, count, operation);

    // Second, 0 broadcasts the result to all ranks
    broadcast(0, rank, recvBuffer, datatype, count, MpiMessageType::ALLREDUCE);
}

void MpiWorld::op_reduce(faabric_op_t* operation,
                         faabric_datatype_t* datatype,
                         int count,
                         uint8_t* inBuffer,
                         uint8_t* outBuffer)
{
    SPDLOG_TRACE(
      "MPI - reduce op: {} datatype {}", operation->id, datatype->id);
    if (operation->id == faabric_op_max.id) {
        if (datatype->id == FAABRIC_INT) {
            auto inBufferCast = reinterpret_cast<int*>(inBuffer);
            auto outBufferCast = reinterpret_cast<int*>(outBuffer);

            for (int slot = 0; slot < count; slot++) {
                outBufferCast[slot] =
                  std::max<int>(outBufferCast[slot], inBufferCast[slot]);
            }
        } else if (datatype->id == FAABRIC_UINT64) {
            auto inBufferCast = reinterpret_cast<uint64_t*>(inBuffer);
            auto outBufferCast = reinterpret_cast<uint64_t*>(outBuffer);

            for (int slot = 0; slot < count; slot++) {
                outBufferCast[slot] =
                  std::max<uint64_t>(outBufferCast[slot], inBufferCast[slot]);
            }
        } else if (datatype->id == FAABRIC_DOUBLE) {
            auto inBufferCast = reinterpret_cast<double*>(inBuffer);
            auto outBufferCast = reinterpret_cast<double*>(outBuffer);

            for (int slot = 0; slot < count; slot++) {
                outBufferCast[slot] =
                  std::max<double>(outBufferCast[slot], inBufferCast[slot]);
            }
        } else if (datatype->id == FAABRIC_LONG_LONG) {
            auto inBufferCast = reinterpret_cast<long long*>(inBuffer);
            auto outBufferCast = reinterpret_cast<long long*>(outBuffer);

            for (int slot = 0; slot < count; slot++) {
                outBufferCast[slot] =
                  std::max<long long>(outBufferCast[slot], inBufferCast[slot]);
            }
        } else {
            SPDLOG_ERROR("Unsupported type for max reduction (datatype={})",
                         datatype->id);
            throw std::runtime_error("Unsupported type for max reduction");
        }
    } else if (operation->id == faabric_op_min.id) {
        if (datatype->id == FAABRIC_INT) {
            auto inBufferCast = reinterpret_cast<int*>(inBuffer);
            auto outBufferCast = reinterpret_cast<int*>(outBuffer);

            for (int slot = 0; slot < count; slot++) {
                outBufferCast[slot] =
                  std::min<int>(outBufferCast[slot], inBufferCast[slot]);
            }
        } else if (datatype->id == FAABRIC_UINT64) {
            auto inBufferCast = reinterpret_cast<uint64_t*>(inBuffer);
            auto outBufferCast = reinterpret_cast<uint64_t*>(outBuffer);

            for (int slot = 0; slot < count; slot++) {
                outBufferCast[slot] =
                  std::min<uint64_t>(outBufferCast[slot], inBufferCast[slot]);
            }
        } else if (datatype->id == FAABRIC_DOUBLE) {
            auto inBufferCast = reinterpret_cast<double*>(inBuffer);
            auto outBufferCast = reinterpret_cast<double*>(outBuffer);

            for (int slot = 0; slot < count; slot++) {
                outBufferCast[slot] =
                  std::min<double>(outBufferCast[slot], inBufferCast[slot]);
            }
        } else if (datatype->id == FAABRIC_LONG_LONG) {
            auto inBufferCast = reinterpret_cast<long long*>(inBuffer);
            auto outBufferCast = reinterpret_cast<long long*>(outBuffer);

            for (int slot = 0; slot < count; slot++) {
                outBufferCast[slot] =
                  std::min<long long>(outBufferCast[slot], inBufferCast[slot]);
            }
        } else {
            SPDLOG_ERROR("Unsupported type for min reduction (datatype={})",
                         datatype->id);
            throw std::runtime_error("Unsupported type for min reduction");
        }
    } else if (operation->id == faabric_op_sum.id) {
        if (datatype->id == FAABRIC_INT) {
            auto inBufferCast = reinterpret_cast<int*>(inBuffer);
            auto outBufferCast = reinterpret_cast<int*>(outBuffer);

            for (int slot = 0; slot < count; slot++) {
                outBufferCast[slot] += inBufferCast[slot];
            }
        } else if (datatype->id == FAABRIC_UINT64) {
            auto inBufferCast = reinterpret_cast<uint64_t*>(inBuffer);
            auto outBufferCast = reinterpret_cast<uint64_t*>(outBuffer);

            for (int slot = 0; slot < count; slot++) {
                outBufferCast[slot] += inBufferCast[slot];
            }
        } else if (datatype->id == FAABRIC_DOUBLE) {
            auto inBufferCast = reinterpret_cast<double*>(inBuffer);
            auto outBufferCast = reinterpret_cast<double*>(outBuffer);

            for (int slot = 0; slot < count; slot++) {
                outBufferCast[slot] += inBufferCast[slot];
            }
        } else if (datatype->id == FAABRIC_LONG_LONG) {
            auto inBufferCast = reinterpret_cast<long long*>(inBuffer);
            auto outBufferCast = reinterpret_cast<long long*>(outBuffer);

            for (int slot = 0; slot < count; slot++) {
                outBufferCast[slot] += inBufferCast[slot];
            }
        } else {
            SPDLOG_ERROR("Unsupported type for sum reduction (datatype={})",
                         datatype->id);
            throw std::runtime_error("Unsupported type for sum reduction");
        }
    } else {
        SPDLOG_ERROR("Reduce operation not implemented: {}", operation->id);
        throw std::runtime_error("Not yet implemented reduce operation");
    }
}

void MpiWorld::scan(int rank,
                    uint8_t* sendBuffer,
                    uint8_t* recvBuffer,
                    faabric_datatype_t* datatype,
                    int count,
                    faabric_op_t* operation)
{
    SPDLOG_TRACE("MPI - scan");

    if (rank > this->size - 1) {
        throw std::runtime_error(
          fmt::format("Rank {} bigger than world size {}", rank, this->size));
    }

    bool isInPlace = sendBuffer == recvBuffer;

    // Scan performs an inclusive prefix reduction, so our input values
    // need also to be considered.
    size_t bufferSize = datatype->size * count;
    if (!isInPlace) {
        ::memcpy(recvBuffer, sendBuffer, bufferSize);
    }

    if (rank > 0) {
        // Receive the current accumulated value
        auto currentAcc = std::make_unique<uint8_t[]>(bufferSize);
        recv(rank - 1,
             rank,
             currentAcc.get(),
             datatype,
             count,
             nullptr,
             MpiMessageType::SCAN);
        // Reduce with our own value
        op_reduce(operation, datatype, count, currentAcc.get(), recvBuffer);
    }

    // If not the last process, send to the next one
    if (rank < this->size - 1) {
        send(rank, rank + 1, recvBuffer, MPI_INT, count, MpiMessageType::SCAN);
    }
}

void MpiWorld::allToAll(int rank,
                        uint8_t* sendBuffer,
                        faabric_datatype_t* sendType,
                        int sendCount,
                        uint8_t* recvBuffer,
                        faabric_datatype_t* recvType,
                        int recvCount)
{
    checkSendRecvMatch(sendType, sendCount, recvType, recvCount);

    // Non-locality-aware implementation
    size_t sendOffset = sendCount * sendType->size;

    // Send out messages for this rank
    for (int recvRank = 0; recvRank < size; recvRank++) {
        // Work out what data to send to this rank
        size_t rankOffset = recvRank * sendOffset;
        uint8_t* sendChunk = sendBuffer + rankOffset;

        if (recvRank == rank) {
            // Copy directly
            std::copy(
              sendChunk, sendChunk + sendOffset, recvBuffer + rankOffset);
        } else {
            // Send message to other rank
            send(rank,
                 recvRank,
                 sendChunk,
                 sendType,
                 sendCount,
                 MpiMessageType::ALLTOALL);
        }
    }

    // Await incoming messages from others
    for (int sendRank = 0; sendRank < size; sendRank++) {
        if (sendRank == rank) {
            continue;
        }

        // Work out where to place the result from this rank
        uint8_t* recvChunk = recvBuffer + (sendRank * sendOffset);

        // Do the receive
        recv(sendRank,
             rank,
             recvChunk,
             recvType,
             recvCount,
             nullptr,
             MpiMessageType::ALLTOALL);
    }

    /* 25/03/2024 - Temporarily disable the locality-aware all-to-all
     * implementation as it is not clear if the reduction of cross-VM messages
     * justifies the increase in local messages (by a factor of 3) plus the
     * contention on local leaders.
     *
    size_t sendSize = sendCount * sendType->size;
    size_t recvSize = recvCount * recvType->size;
    assert(sendSize == recvSize);
    assert(sendType->size == recvType->size);
    assert(sendCount == recvCount);

    // When we are sending messages, this method gives us the offset in
    // sendBuffer for a given receiver rank
    auto getSendBufferOffset = [sendSize](int recvRank) {
        return recvRank * sendSize;
    };

    // When we are receiving messages, this method gives us the offset in
    // the recvBuffer for a given sender rank
    auto getRecvBufferOffset = [recvSize](int sendRank) {
        return sendRank * recvSize;
    };

    // Given a send idx and a recv idx, get the offset into the batched
    // message buffer
    auto getBatchMsgOffset =
      [sendSize](int recvWorldSize, int sendIdx, int recvIdx) {
          return (recvWorldSize * sendIdx + recvIdx) * sendSize;
      };

    // -------- Send Phase --------

    // First, just send our local messages to our local ranks
    for (const int recvRank : ranksForHost.at(thisHost)) {
        uint8_t* sendChunk = sendBuffer + getSendBufferOffset(recvRank);

        if (recvRank == rank) {
            // Copy directly
            std::memcpy(
              recvBuffer + getRecvBufferOffset(rank), sendChunk, sendSize);
        } else {
            send(rank,
                 recvRank,
                 sendChunk,
                 sendType,
                 sendCount,
                 MpiMessageType::ALLTOALL);
        }
    }

    if (rank == localLeader) {
        // Given that we _always_ send local messages first, the local leader
        // needs to receive them here. Otherwise subsequent recv's would be
        // out of order
        for (const int sendRank : ranksForHost.at(thisHost)) {
            if (sendRank == rank) {
                // We have  already copied the chunk in before
                continue;
            }

            recv(sendRank,
                 rank,
                 recvBuffer + getRecvBufferOffset(sendRank),
                 recvType,
                 recvCount,
                 nullptr,
                 MpiMessageType::ALLTOALL);
        }

        // The local leader now receives, for each remote world,
        // `remoteWorldSize` messages from each local rank, and it batches
        // them into one network message per remote world
        for (const auto& [remoteHost, remoteRanks] : ranksForHost) {
            if (remoteHost == thisHost) {
                continue;
            }

            auto localRanks = ranksForHost.at(thisHost);
            int remoteLeader = *(remoteRanks.begin());

            // Prepare a the batch message
            int numMessages =
              localRanks.size() * remoteRanks.size() * sendCount;
            size_t bufferSize = numMessages * sendSize;
            MpiMessage remoteBatchMsg = { .worldId = id,
                                          .sendRank = rank,
                                          .recvRank = remoteLeader,
                                          .typeSize = (int)sendSize,
                                          .count = numMessages,
                                          .messageType =
                                            MpiMessageType::ALLTOALL_PACKED,
                                          .buffer =
                                            faabric::util::malloc(bufferSize) };

            // Receive `remoteRanks` message from each local rank, and copy
            // them to the appropriate offset in the buffer
            auto localItr = localRanks.begin();
            for (int sendIdx = 0; sendIdx < localRanks.size();
                 sendIdx++, localItr++) {
                int localSendRank = (*localItr);

                auto remoteItr = remoteRanks.begin();
                for (int recvIdx = 0; recvIdx < remoteRanks.size();
                     recvIdx++, remoteItr++) {
                    int remoteRecvRank = (*remoteItr);
                    size_t batchOffset =
                      getBatchMsgOffset(remoteRanks.size(), sendIdx, recvIdx);

                    if (localSendRank == rank) {
                        std::memcpy(
                          (uint8_t*)remoteBatchMsg.buffer + batchOffset,
                          sendBuffer + getSendBufferOffset(remoteRecvRank),
                          sendSize);

                        continue;
                    }

                    // Receive directly into the appropriate buffer
                    recv(localSendRank,
                         rank,
                         (uint8_t*)remoteBatchMsg.buffer + batchOffset,
                         sendType,
                         sendCount,
                         nullptr,
                         MpiMessageType::ALLTOALL);
                }
            }

            // Send the remote message
            sendRemoteMpiMessage(
              remoteHost, rank, remoteLeader, remoteBatchMsg);
        }
    } else {
        // If we are not the local leader, we just send our local leader all
        // our remote messages
        for (const auto& [remoteHost, remoteRanks] : ranksForHost) {
            if (remoteHost == thisHost) {
                continue;
            }

            // The local leader will infer the recv rank from the order
            // in which we are sending the messages
            for (const int recvRank : remoteRanks) {
                send(rank,
                     localLeader,
                     sendBuffer + getSendBufferOffset(recvRank),
                     sendType,
                     sendCount,
                     MpiMessageType::ALLTOALL);
            }
        }
    }

    // -------- Receive Phase --------

    if (rank == localLeader) {
        // If we are a local leader, we now need to receive one remote batch
        // message per remote host, and propagate the messages downstream
        for (const auto& [remoteHost, remoteRanks] : ranksForHost) {
            if (remoteHost == thisHost) {
                continue;
            }

            auto localRanks = ranksForHost.at(thisHost);
            int remoteLeader = *(remoteRanks.begin());

            auto remoteBatchMsg = getLocalQueue(remoteLeader, rank)->dequeue();

            // Sanity check the remote (batched) message
            [[maybe_unused]] int numMessages =
              localRanks.size() * remoteRanks.size() * recvCount;
            assert(remoteBatchMsg.count == numMessages);
            assert(remoteBatchMsg.typeSize == (int)sendSize);
            assert(remoteBatchMsg.messageType ==
                   MpiMessageType::ALLTOALL_PACKED);

            // For each local rank, send remoteWorldSize messages
            auto localItr = localRanks.begin();
            for (int recvIdx = 0; recvIdx < localRanks.size();
                 recvIdx++, localItr++) {
                int localRecvRank = *localItr;

                auto remoteItr = remoteRanks.begin();
                for (int sendIdx = 0; sendIdx < remoteRanks.size();
                     sendIdx++, remoteItr++) {
                    int remoteSendRank = *remoteItr;
                    size_t batchOffset =
                      getBatchMsgOffset(localRanks.size(), sendIdx, recvIdx);

                    if (localRecvRank == rank) {
                        std::memcpy(
                          recvBuffer + getRecvBufferOffset(remoteSendRank),
                          (uint8_t*)remoteBatchMsg.buffer + batchOffset,
                          recvSize);
                        continue;
                    }

                    // The local rank is expecting to receive from the local
                    // leader. It is the message ordering that will let the
                    // local rank know which remote rank it corresponds to
                    send(rank,
                         localRecvRank,
                         (uint8_t*)remoteBatchMsg.buffer + batchOffset,
                         sendType,
                         sendCount,
                         MpiMessageType::ALLTOALL);
                }
           }
        }
    } else {
        // First, receive our local messages from our local ranks
        for (const int sendRank : ranksForHost.at(thisHost)) {
            if (sendRank == rank) {
                // We have  already copied the chunk in before
                continue;
            }

            recv(sendRank,
                 rank,
                 recvBuffer + getRecvBufferOffset(sendRank),
                 recvType,
                 recvCount,
                 nullptr,
                 MpiMessageType::ALLTOALL);
        }

        // If we are not the local leader, we receive all non-local messages
        // from our local leader
        for (const auto& [remoteHost, remoteRanks] : ranksForHost) {
            if (remoteHost == thisHost) {
                continue;
            }

            for (const int sendRank : remoteRanks) {
                recv(localLeader,
                     rank,
                     recvBuffer + getRecvBufferOffset(sendRank),
                     recvType,
                     recvCount,
                     nullptr,
                     MpiMessageType::ALLTOALL);
            }
        }
    }
    */
}

// 30/12/21 - Probe is now broken after the switch to a different type of
// queues for local messaging. New queues don't support (off-the-shelf) the
// ability to return a reference to the first element in the queue. In order
// to re-include support for probe we must fix the peek method in the
// queues.
void MpiWorld::probe(int sendRank, int recvRank, MPI_Status* status)
{
    throw std::runtime_error("Probe not implemented!");
    /*
    const std::shared_ptr<InMemoryMpiQueue>& queue =
      getLocalQueue(sendRank, recvRank);
    std::shared_ptr<MPIMessage> m = *(queue->peek());

    faabric_datatype_t* datatype = getFaabricDatatypeFromId(m->type());
    status->bytesSize = m->count() * datatype->size;
    status->MPI_ERROR = 0;
    status->MPI_SOURCE = m->sender();
    */
}

void MpiWorld::barrier(int thisRank)
{
    // Rank 0 coordinates the barrier operation
    if (thisRank == 0) {
        // This is the root, hence waits for all ranks to get to the barrier
        SPDLOG_TRACE("MPI - barrier init {}", thisRank);

        // Await messages from all others
        for (int r = 1; r < size; r++) {
            MPI_Status s{};
            recv(r, 0, nullptr, MPI_INT, 0, &s, MpiMessageType::BARRIER_JOIN);
            SPDLOG_TRACE("MPI - recv barrier join {}", s.MPI_SOURCE);
        }
    } else {
        // Tell the root that we're waiting
        SPDLOG_TRACE("MPI - barrier join {}", thisRank);
        send(thisRank, 0, nullptr, MPI_INT, 0, MpiMessageType::BARRIER_JOIN);
    }

    // Rank 0 broadcasts that the barrier is done (the others block here)
    broadcast(0, thisRank, nullptr, MPI_INT, 0, MpiMessageType::BARRIER_DONE);
    SPDLOG_TRACE("MPI - barrier done {}", thisRank);
}

std::shared_ptr<InMemoryMpiQueue> MpiWorld::getLocalQueue(int sendRank,
                                                          int recvRank)
{
    assert(getHostForRank(recvRank) == thisHost);
    assert(localQueues.size() == size * size);

    return localQueues[getIndexForRanks(sendRank, recvRank)];
}

void MpiWorld::initRecvThread()
{
    assert(rankState.msg != nullptr);
    int thisRank = rankState.msg->groupidx();
    int thisPort = getPortForRank(thisRank);

    // This method is called twice from remote local leaders (once as part of
    // initialiseFromMsg, and another one from setMsgForRank) as a consequence
    // we skip initialisation if the thread is already initialised
    if (rankState.recvThread != nullptr) {
        return;
    }

    rankState.recvThread =
      std::make_unique<std::jthread>([this, thisRank, thisPort]() {
          // Variable is only used in debug builds, so mark it here to avoid
          // compiler warnigns
          (void)thisRank;

          SPDLOG_DEBUG(
            "MPI - Initialising BG receiver thread for rank {} (port: {})",
            thisRank,
            thisPort);

          faabric::transport::AsyncRecvMessageEndpoint recvSocket(thisPort);
          auto stopToken = rankState.stopSource.get_token();

          while (!stopToken.stop_requested()) {
              auto msg = recvSocket.recv();

              // On timeout we listen again
              if (msg.getResponseCode() ==
                  faabric::transport::MessageResponseCode::TIMEOUT) {
                  continue;
              }

              // TODO: can we avoid this copy here?
              MpiMessage parsedMsg;
              parseMpiMsg(msg.dataCopy(), &parsedMsg);

              if (parsedMsg.messageType == SHUTDOWN_WORLD) {
                  break;
              }

              getLocalQueue(parsedMsg.sendRank, parsedMsg.recvRank)
                ->enqueue(parsedMsg);
          }

          SPDLOG_DEBUG(
            "MPI - Shutting down BG receiver thread for rank {} (port: {})",
            thisRank,
            thisPort);
      });
}

void MpiWorld::stopRecvThread()
{
    if (rankState.recvThread == nullptr) {
        return;
    }

    // To properly shut down our receiver TCP socket, we request stopping the
    // jthread, and then send an empty (shutdown) message to ourselves
    rankState.stopSource.request_stop();
    sendRemoteMpiMessage(thisHost,
                         rankState.msg->mpirank(),
                         rankState.msg->mpirank(),
                         MPI_SHUTDOWN_MESSAGE);

    rankState.recvThread->join();
    rankState.recvThread.reset();
}

// We pre-allocate all _potentially_ necessary queues in advance. Queues are
// necessary to _receive_ messages, thus we initialise all queues whose
// corresponding receiver is local to this host, for any sender
void MpiWorld::initLocalQueues()
{
    localQueues.resize(size * size);
    for (int sendRank = 0; sendRank < size; sendRank++) {
        for (const int recvRank : ranksForHost[thisHost]) {
            // We handle messages-to-self as memory copies
            if (sendRank == recvRank) {
                continue;
            }

            if (localQueues[getIndexForRanks(sendRank, recvRank)] == nullptr) {
                localQueues[getIndexForRanks(sendRank, recvRank)] =
                  std::make_shared<InMemoryMpiQueue>();
            }
        }
    }
}

MpiMessage MpiWorld::recvBatchReturnLast(int sendRank,
                                         int recvRank,
                                         int requestId)
{
    auto index = getUnackedMessageBuffer(sendRank, recvRank);

    // Work out whether the message is sent locally or from another host
    assert(thisHost == getHostForRank(recvRank));
    const std::string otherHost = getHostForRank(sendRank);

    // Fast-path for non-async case
    if (rankState.unackedMessageBuffers.at(index)->empty()) {
        return getLocalQueue(sendRank, recvRank)->dequeue();
    }

    // If there are Irecv-ed messages pending to be acked, read them from the
    // transport
    auto itr = rankState.unackedMessageBuffers.at(index)->begin();
    while (itr != rankState.unackedMessageBuffers.at(index)->end()) {
        // We are receiving a message that has been previously irecv-ed, so
        // we can safely copy into the provided buffer, and free the received
        // one
        if (itr->messageType == MpiMessageType::UNACKED_MPI_MESSAGE) {
            SPDLOG_TRACE("MPI - pending recv {} -> {}", sendRank, recvRank);

            // Copy the request id so that it is not overwritten
            int tmpRequestId = itr->requestId;

            // Copy into current slot in the list, but keep a copy to the
            // app-provided buffer to read data into
            void* providedBuffer = itr->buffer;
            *itr = getLocalQueue(sendRank, recvRank)->dequeue();
            itr->requestId = tmpRequestId;

            if (itr->buffer != nullptr) {
                assert(providedBuffer != nullptr);
                // If buffers are not null, we must have a non-zero size
                assert((itr->count * itr->typeSize) > 0);
                std::memcpy(
                  providedBuffer, itr->buffer, itr->count * itr->typeSize);
                faabric::util::free(itr->buffer);
            }
            itr->buffer = providedBuffer;
        }
        assert(itr->messageType != MpiMessageType::UNACKED_MPI_MESSAGE);

        // If called from await, we are given a request id such that, when
        // we find it, we can return
        if (requestId == itr->requestId) {
            break;
        }

        itr++;
    }

    // If we have breaked-out of the while, it means that we found what we
    // were looking for and we are done
    if (itr != rankState.unackedMessageBuffers.at(index)->end()) {
        assert(requestId == itr->requestId);

        rankState.unackedMessageBuffers.at(index)->erase(itr);
        return MpiMessage{};
    }

    // Lastly, return our message
    return getLocalQueue(sendRank, recvRank)->dequeue();
}

int MpiWorld::getIndexForRanks(int sendRank, int recvRank) const
{
    int index = sendRank * size + recvRank;
    assert(index >= 0 && index < size * size);
    return index;
}

double MpiWorld::getWTime()
{
    double t = faabric::util::getTimeDiffMillis(creationTime);
    return t / 1000.0;
}

std::string MpiWorld::getUser()
{
    return user;
}

std::string MpiWorld::getFunction()
{
    return function;
}

int MpiWorld::getId() const
{
    return id;
}

int MpiWorld::getSize() const
{
    return size;
}

void MpiWorld::overrideHost(const std::string& newHost)
{
    thisHost = newHost;
}

void MpiWorld::checkRanksRange(int sendRank, int recvRank)
{
    if (sendRank < 0 || sendRank >= size) {
        SPDLOG_ERROR(
          "Send rank outside range: {} not in [0, {})", sendRank, size);
        throw std::runtime_error("Send rank outside range");
    }
    if (recvRank < 0 || recvRank >= size) {
        SPDLOG_ERROR(
          "Recv rank outside range: {} not in [0, {})", recvRank, size);
        throw std::runtime_error("Recv rank outside range");
    }
}

void MpiWorld::prepareMigration(int thisRank, bool thisRankMustMigrate)
{
    // Check that there are no pending asynchronous messages to send and receive
    auto itr = rankState.unackedMessageBuffers.begin();
    while (itr != rankState.unackedMessageBuffers.end()) {
        if (*itr != nullptr && !(*itr)->empty()) {
            SPDLOG_ERROR("Trying to migrate MPI application (id: {}) but rank"
                         " {} has {} pending async messages to receive",
                         rankState.msg->appid(),
                         thisRank,
                         (*itr)->size());
            throw std::runtime_error(
              "Migrating with pending async messages is not supported");
        }

        itr++;
    }

    // If our (this rank) is being migrated, stop the receiver thread
    if (thisRankMustMigrate) {
        stopRecvThread();
    }

    // Clear our TLS sockets to force lazy-initialising them after migration
    // so that we pick up the right ranks for the remote ranks
    rankState.sendSockets.clear();

    // Update local records
    if (thisRank == localLeader) {
        initLocalRemoteLeaders();

        // Add the necessary new local messaging queues
        initLocalQueues();
    }
}
}
