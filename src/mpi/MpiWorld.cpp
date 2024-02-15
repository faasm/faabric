#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/mpi/MpiWorld.h>
#include <faabric/mpi/mpi.pb.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/transport/macros.h>
#include <faabric/util/ExecGraph.h>
#include <faabric/util/batch.h>
#include <faabric/util/environment.h>
#include <faabric/util/gids.h>
#include <faabric/util/macros.h>
#include <faabric/util/testing.h>

// Each MPI rank runs in a separate thread, thus we use TLS to maintain the
// per-rank data structures
static thread_local std::vector<std::shared_ptr<faabric::mpi::MpiMessageBuffer>>
  unackedMessageBuffers;

static thread_local std::set<int> iSendRequests;

static thread_local std::map<int, std::pair<int, int>> reqIdToRanks;

static thread_local int localMsgCount = 1;

// Id of the message that created this thread-local instance
static thread_local faabric::Message* thisRankMsg = nullptr;

namespace faabric::mpi {

// -----------------------------------
// Mocking
// -----------------------------------
static std::mutex mockMutex;

// The identifier in this map is the sending rank. For the receiver's rank
// we can inspect the MPIMessage object
static std::map<int, std::vector<std::shared_ptr<MPIMessage>>>
  mpiMockedMessages;

std::vector<std::shared_ptr<MPIMessage>> getMpiMockedMessages(int sendRank)
{
    faabric::util::UniqueLock lock(mockMutex);
    return mpiMockedMessages[sendRank];
}

MpiWorld::MpiWorld()
  : thisHost(faabric::util::getSystemConfig().endpointHost)
  , creationTime(faabric::util::startTimer())
  , cartProcsPerDim(2)
  , broker(faabric::transport::getPointToPointBroker())
{}

void MpiWorld::sendRemoteMpiMessage(std::string dstHost,
                                    int sendRank,
                                    int recvRank,
                                    const std::shared_ptr<MPIMessage>& msg)
{
    std::string serialisedBuffer;
    if (!msg->SerializeToString(&serialisedBuffer)) {
        throw std::runtime_error("Error serialising message");
    }
    try {
        broker.sendMessage(
          thisRankMsg->groupid(),
          sendRank,
          recvRank,
          reinterpret_cast<const uint8_t*>(serialisedBuffer.data()),
          serialisedBuffer.size(),
          dstHost,
          true);
    } catch (std::runtime_error& e) {
        SPDLOG_ERROR("{}:{}:{} Timed out with: MPI - send {} -> {}",
                     thisRankMsg->appid(),
                     thisRankMsg->groupid(),
                     thisRankMsg->groupidx(),
                     sendRank,
                     recvRank);
        throw e;
    }
}

std::shared_ptr<MPIMessage> MpiWorld::recvRemoteMpiMessage(int sendRank,
                                                           int recvRank)
{
    std::vector<uint8_t> msg;
    try {
        msg =
          broker.recvMessage(thisRankMsg->groupid(), sendRank, recvRank, true);
    } catch (std::runtime_error& e) {
        SPDLOG_ERROR("{}:{}:{} Timed out with: MPI - recv (remote) {} -> {}",
                     thisRankMsg->appid(),
                     thisRankMsg->groupid(),
                     thisRankMsg->groupidx(),
                     sendRank,
                     recvRank);
        throw e;
    }
    PARSE_MSG(MPIMessage, msg.data(), msg.size());
    return std::make_shared<MPIMessage>(parsedMsg);
}

std::shared_ptr<MpiMessageBuffer> MpiWorld::getUnackedMessageBuffer(
  int sendRank,
  int recvRank)
{
    // We want to lazily initialise this data structure because, given its
    // thread local nature, we expect it to be quite sparse (i.e. filled with
    // nullptr).
    if (unackedMessageBuffers.empty()) {
        unackedMessageBuffers.resize(size * size, nullptr);
    }

    // Get the index for the rank-host pair
    int index = getIndexForRanks(sendRank, recvRank);
    assert(index >= 0 && index < size * size);

    if (unackedMessageBuffers[index] == nullptr) {
        unackedMessageBuffers.at(index) = std::make_shared<MpiMessageBuffer>();
    }

    return unackedMessageBuffers[index];
}

void MpiWorld::create(faabric::Message& call, int newId, int newSize)
{
    id = newId;
    user = call.user();
    function = call.function();
    thisRankMsg = &call;
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
        if (thisRankMsg != nullptr) {
            // Set message fields to allow for function migration
            msg.set_appid(thisRankMsg->appid());
            msg.set_cmdline(thisRankMsg->cmdline());
            msg.set_inputdata(thisRankMsg->inputdata());

            // Log chained functions to generate execution graphs
            if (thisRankMsg->recordexecgraph()) {
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
        thisRankMsg->set_groupid(decision.groupId);
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
}

void MpiWorld::destroy()
{
    SPDLOG_TRACE("Destroying MPI world {}", id);

    // Note that all ranks will call this function.

    // Unacked message buffers
    if (!unackedMessageBuffers.empty()) {
        for (auto& umb : unackedMessageBuffers) {
            if (umb != nullptr) {
                if (!umb->isEmpty()) {
                    SPDLOG_ERROR("Destroying the MPI world with outstanding {}"
                                 " messages in the message buffer",
                                 umb->size());
                    throw std::runtime_error(
                      "Destroying world with a non-empty MPI message buffer");
                }
            }
        }
        unackedMessageBuffers.clear();
    }

    // Request to rank map should be empty
    if (!reqIdToRanks.empty()) {
        SPDLOG_ERROR(
          "Destroying the MPI world with {} outstanding irecv requests",
          reqIdToRanks.size());
        throw std::runtime_error("Destroying world with outstanding requests");
    }

    // iSend set should be empty
    if (!iSendRequests.empty()) {
        SPDLOG_ERROR(
          "Destroying the MPI world with {} outstanding isend requests",
          iSendRequests.size());
        throw std::runtime_error("Destroying world with outstanding requests");
    }

    // Lastly, clear-out the rank message
    thisRankMsg = nullptr;

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
    thisRankMsg = &msg;

    // Record which ranks are local to this world, and query for all leaders
    initLocalRemoteLeaders();

    // Initialise the memory queues for message reception
    initLocalQueues();
}

void MpiWorld::setMsgForRank(faabric::Message& msg)
{
    thisRankMsg = &msg;
}

std::string MpiWorld::getHostForRank(int rank)
{
    // This method may be called a lot, so we query our cached records instead
    // of the point-to-point broker, where we need to acquire a shared lock for
    // every query.
    return hostForRank.at(rank);
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

    // First, group the ranks per host they belong to for convinience. We also
    // keep a record of the opposite mapping, the host that each rank belongs
    // to, as it is queried frequently and asking the ptp broker involves
    // acquiring a lock.
    if (thisRankMsg == nullptr) {
        throw std::runtime_error("Rank message not set!");
    }
    int groupId = thisRankMsg->groupid();
    auto rankIds = broker.getIdxsRegisteredForGroup(groupId);
    if (rankIds.size() != size) {
        SPDLOG_ERROR("{}:{}:{} rankIds != size ({} != {})",
                     thisRankMsg->appid(),
                     groupId,
                     thisRankMsg->groupidx(),
                     rankIds.size(),
                     size);
        throw std::runtime_error("MPI Group-World size mismatch!");
    }
    assert(rankIds.size() == size);
    hostForRank.resize(size);
    for (const auto& rankId : rankIds) {
        std::string host = broker.getHostForReceiver(groupId, rankId);
        ranksForHost[host].push_back(rankId);
        hostForRank.at(rankId) = host;
    }

    // Second, put the local leader for each host (currently lowest rank) at the
    // front.
    for (auto it : ranksForHost) {
        // Persist the local leader in this host for further use
        if (it.first == thisHost) {
            localLeader = *std::min_element(it.second.begin(), it.second.end());
        }

        std::iter_swap(it.second.begin(),
                       std::min_element(it.second.begin(), it.second.end()));
    }
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

// Sending is already asynchronous in both transport layers we use: in-memory
// queues for local messages, and ZeroMQ sockets for remote messages. Thus,
// we can just send normally and return a requestId. Upon await, we'll return
// immediately.
int MpiWorld::isend(int sendRank,
                    int recvRank,
                    const uint8_t* buffer,
                    faabric_datatype_t* dataType,
                    int count,
                    MPIMessage::MPIMessageType messageType)
{
    int requestId = (int)faabric::util::generateGid();
    iSendRequests.insert(requestId);

    send(sendRank, recvRank, buffer, dataType, count, messageType);

    return requestId;
}

int MpiWorld::irecv(int sendRank,
                    int recvRank,
                    uint8_t* buffer,
                    faabric_datatype_t* dataType,
                    int count,
                    MPIMessage::MPIMessageType messageType)
{
    int requestId = (int)faabric::util::generateGid();
    reqIdToRanks.try_emplace(requestId, sendRank, recvRank);

    // Enqueue an unacknowleged request (no message)
    MpiMessageBuffer::PendingAsyncMpiMessage pendingMsg;
    pendingMsg.requestId = requestId;
    pendingMsg.sendRank = sendRank;
    pendingMsg.recvRank = recvRank;
    pendingMsg.buffer = buffer;
    pendingMsg.dataType = dataType;
    pendingMsg.count = count;
    pendingMsg.messageType = messageType;
    assert(!pendingMsg.isAcknowledged());

    auto umb = getUnackedMessageBuffer(sendRank, recvRank);
    umb->addMessage(pendingMsg);

    return requestId;
}

void MpiWorld::send(int sendRank,
                    int recvRank,
                    const uint8_t* buffer,
                    faabric_datatype_t* dataType,
                    int count,
                    MPIMessage::MPIMessageType messageType)
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
    int msgId = (localMsgCount + 1) % INT32_MAX;

    // Create the message
    auto m = std::make_shared<MPIMessage>();
    m->set_id(msgId);
    m->set_worldid(id);
    m->set_sender(sendRank);
    m->set_destination(recvRank);
    m->set_type(dataType->id);
    m->set_count(count);
    m->set_messagetype(messageType);

    // Set up message data
    bool mustSendData = count > 0 && buffer != nullptr;

    // Mock the message sending in tests
    if (faabric::util::isMockMode()) {
        mpiMockedMessages[sendRank].push_back(m);
        return;
    }

    // Dispatch the message locally or globally
    if (isLocal) {
        void* bufferPtr = malloc(count * dataType->size);
        std::memcpy(bufferPtr, buffer, count* dataType->size);

        if (mustSendData) {
            m->set_bufferptr((uint64_t)bufferPtr);
        }
        SPDLOG_INFO("Send (Ptr: {} - Size: {} - Data as int: {})", m->bufferptr(), count * dataType->size, ((int*)m->bufferptr())[0]);

        SPDLOG_TRACE(
          "MPI - send {} -> {} ({})", sendRank, recvRank, messageType);
        getLocalQueue(sendRank, recvRank)->enqueue(std::move(m));
    } else {
        if (mustSendData) {
            m->set_buffer(buffer, dataType->size * count);
        }

        SPDLOG_TRACE(
          "MPI - send remote {} -> {} ({})", sendRank, recvRank, messageType);
        sendRemoteMpiMessage(otherHost, sendRank, recvRank, m);
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
                    MPIMessage::MPIMessageType messageType)
{
    // Sanity-check input parameters
    checkRanksRange(sendRank, recvRank);

    // If mocking the messages, ignore calls to receive that may block
    if (faabric::util::isMockMode()) {
        return;
    }

    // Recv message from underlying transport
    std::shared_ptr<MPIMessage> m = recvBatchReturnLast(sendRank, recvRank);

    // Do the processing
    doRecv(m, buffer, dataType, count, status, messageType);
}

void MpiWorld::doRecv(std::shared_ptr<MPIMessage>& m,
                      uint8_t* buffer,
                      faabric_datatype_t* dataType,
                      int count,
                      MPI_Status* status,
                      MPIMessage::MPIMessageType messageType)
{
    // Assert message integrity
    // Note - this checks won't happen in Release builds
    if (m->messagetype() != messageType) {
        SPDLOG_ERROR("Different message types (got: {}, expected: {})",
                     m->messagetype(),
                     messageType);
    }
    assert(m->messagetype() == messageType);
    assert(m->count() <= count);

    const std::string otherHost = getHostForRank(m->destination());
    bool isLocal = otherHost == thisHost;

    if (m->count() > 0) {
        if (isLocal) {
            SPDLOG_INFO("Recv (Ptr: {} - Size: {} - Data as int: {})", m->bufferptr(), count * dataType->size, ((int*)m->bufferptr())[0]);
            std::memcpy(buffer, (void*)m->bufferptr(), count * dataType->size);
            free((void*)m->bufferptr());
        } else {
            // TODO - avoid copy here
            std::move(m->buffer().begin(), m->buffer().end(), buffer);
        }
    }

    // Set status values if required
    if (status != nullptr) {
        status->MPI_SOURCE = m->sender();
        status->MPI_ERROR = MPI_SUCCESS;

        // Take the message size here as the receive count may be larger
        status->bytesSize = m->count() * dataType->size;

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

    if (recvRank > this->size - 1) {
        throw std::runtime_error(fmt::format(
          "Receive rank {} bigger than world size {}", recvRank, this->size));
    }
    if (sendRank > this->size - 1) {
        throw std::runtime_error(fmt::format(
          "Send rank {} bigger than world size {}", sendRank, this->size));
    }

    // Post async recv
    int recvId = irecv(recvRank,
                       myRank,
                       recvBuffer,
                       recvDataType,
                       recvCount,
                       MPIMessage::SENDRECV);
    // Then send the message
    send(myRank,
         sendRank,
         sendBuffer,
         sendDataType,
         sendCount,
         MPIMessage::SENDRECV);
    // And wait
    awaitAsyncRequest(recvId);
}

void MpiWorld::broadcast(int sendRank,
                         int recvRank,
                         uint8_t* buffer,
                         faabric_datatype_t* dataType,
                         int count,
                         MPIMessage::MPIMessageType messageType)
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
                     it.second.front(),
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
                     MPIMessage::SCATTER);
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
             MPIMessage::SCATTER);
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
                             MPIMessage::GATHER);
                    }
                }
            } else {
                // Receive from remote local leaders their local gathered data
                auto rankData =
                  std::make_unique<uint8_t[]>(it.second.size() * recvSize);

                recv(it.second.front(),
                     recvRank,
                     rankData.get(),
                     recvType,
                     recvCount * it.second.size(),
                     nullptr,
                     MPIMessage::GATHER);

                // Copy each received chunk to its offset
                for (int r = 0; r < it.second.size(); r++) {
                    ::memcpy(recvBuffer + (it.second.at(r) * recvSize),
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
        for (int r = 0; r < ranksForHost[thisHost].size(); r++) {
            if (ranksForHost[thisHost].at(r) == sendRank) {
                // Receive from ourselves, just copy from/to the right offset
                ::memcpy(rankData.get() + r * sendSize,
                         sendBuffer + sendBufferOffset,
                         sendSize);
            } else {
                // Receive from other local ranks
                recv(ranksForHost[thisHost].at(r),
                     sendRank,
                     rankData.get() + r * sendSize,
                     sendType,
                     sendCount,
                     nullptr,
                     MPIMessage::GATHER);
            }
        }

        // Send the locally-gathered data to the receiver rank
        send(sendRank,
             recvRank,
             rankData.get(),
             sendType,
             sendCount * ranksForHost[thisHost].size(),
             MPIMessage::GATHER);

    } else if (isLocalLeader && isLocalGather) {
        // Scenario 3
        send(sendRank,
             recvRank,
             sendBuffer + sendBufferOffset,
             sendType,
             sendCount,
             MPIMessage::GATHER);
    } else if (!isLocalLeader && !isLocalGather) {
        // Scenario 4
        send(sendRank,
             localLeader,
             sendBuffer + sendBufferOffset,
             sendType,
             sendCount,
             MPIMessage::GATHER);
    } else if (!isLocalLeader && isLocalGather) {
        // Scenario 5
        send(sendRank,
             recvRank,
             sendBuffer + sendBufferOffset,
             sendType,
             sendCount,
             MPIMessage::GATHER);
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
      root, rank, recvBuffer, recvType, fullCount, MPIMessage::ALLGATHER);
}

void MpiWorld::awaitAsyncRequest(int requestId)
{
    SPDLOG_TRACE("MPI - await {}", requestId);

    auto iSendIt = iSendRequests.find(requestId);
    if (iSendIt != iSendRequests.end()) {
        iSendRequests.erase(iSendIt);
        return;
    }

    // Get the corresponding send and recv ranks
    auto it = reqIdToRanks.find(requestId);
    // If the request id is not in the map, the application either has issued an
    // await without a previous isend/irecv, or the actual request id
    // has been corrupted. In any case, we error out.
    if (it == reqIdToRanks.end()) {
        SPDLOG_ERROR("Asynchronous request id not recognized: {}", requestId);
        throw std::runtime_error("Unrecognized async request id");
    }
    int sendRank = it->second.first;
    int recvRank = it->second.second;
    reqIdToRanks.erase(it);

    std::shared_ptr<MpiMessageBuffer> umb =
      getUnackedMessageBuffer(sendRank, recvRank);

    std::list<MpiMessageBuffer::PendingAsyncMpiMessage>::iterator msgIt =
      umb->getRequestPendingMsg(requestId);

    std::shared_ptr<MPIMessage> m;
    if (msgIt->msg != nullptr) {
        // This id has already been acknowledged by a recv call, so do the recv
        m = msgIt->msg;
    } else {
        // We need to acknowledge all messages not acknowledged from the
        // begining until us
        m = recvBatchReturnLast(
          sendRank, recvRank, umb->getTotalUnackedMessagesUntil(msgIt) + 1);
    }

    doRecv(m,
           msgIt->buffer,
           msgIt->dataType,
           msgIt->count,
           MPI_STATUS_IGNORE,
           msgIt->messageType);

    // Remove the acknowledged indexes from the UMB
    umb->deleteMessage(msgIt);
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
                         MPIMessage::REDUCE);

                    op_reduce(
                      operation, datatype, count, rankData.get(), recvBuffer);
                }
            } else {
                // For remote ranks, only receive from the host leader
                memset(rankData.get(), 0, bufferSize);
                recv(it.second.front(),
                     recvRank,
                     rankData.get(),
                     datatype,
                     count,
                     nullptr,
                     MPIMessage::REDUCE);

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
                     MPIMessage::REDUCE);

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
                 MPIMessage::REDUCE);
        } else {
            // Send to the receiver rank
            send(sendRank,
                 recvRank,
                 sendBuffer,
                 datatype,
                 count,
                 MPIMessage::REDUCE);
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
             MPIMessage::REDUCE);
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
    broadcast(0, rank, recvBuffer, datatype, count, MPIMessage::ALLREDUCE);
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
             MPIMessage::SCAN);
        // Reduce with our own value
        op_reduce(operation, datatype, count, currentAcc.get(), recvBuffer);
    }

    // If not the last process, send to the next one
    if (rank < this->size - 1) {
        send(rank, rank + 1, recvBuffer, MPI_INT, count, MPIMessage::SCAN);
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

    size_t sendOffset = sendCount * sendType->size;

    // Send out messages for this rank
    for (int r = 0; r < size; r++) {
        // Work out what data to send to this rank
        size_t rankOffset = r * sendOffset;
        uint8_t* sendChunk = sendBuffer + rankOffset;

        if (r == rank) {
            // Copy directly
            std::copy(
              sendChunk, sendChunk + sendOffset, recvBuffer + rankOffset);
        } else {
            // Send message to other rank
            send(rank, r, sendChunk, sendType, sendCount, MPIMessage::ALLTOALL);
        }
    }

    // Await incoming messages from others
    for (int r = 0; r < size; r++) {
        if (r == rank) {
            continue;
        }

        // Work out where to place the result from this rank
        uint8_t* recvChunk = recvBuffer + (r * sendOffset);

        // Do the receive
        recv(r,
             rank,
             recvChunk,
             recvType,
             recvCount,
             nullptr,
             MPIMessage::ALLTOALL);
    }
}

// 30/12/21 - Probe is now broken after the switch to a different type of
// queues for local messaging. New queues don't support (off-the-shelf) the
// ability to return a reference to the first element in the queue. In order
// to re-include support for probe we must fix the peek method in the
// queues.
void MpiWorld::probe(int sendRank, int recvRank, MPI_Status* status)
{
    const std::shared_ptr<InMemoryMpiQueue>& queue =
      getLocalQueue(sendRank, recvRank);
    // 30/12/21 - Peek will throw a runtime error
    std::shared_ptr<MPIMessage> m = *(queue->peek());

    faabric_datatype_t* datatype = getFaabricDatatypeFromId(m->type());
    status->bytesSize = m->count() * datatype->size;
    status->MPI_ERROR = 0;
    status->MPI_SOURCE = m->sender();
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
            recv(r, 0, nullptr, MPI_INT, 0, &s, MPIMessage::BARRIER_JOIN);
            SPDLOG_TRACE("MPI - recv barrier join {}", s.MPI_SOURCE);
        }
    } else {
        // Tell the root that we're waiting
        SPDLOG_TRACE("MPI - barrier join {}", thisRank);
        send(thisRank, 0, nullptr, MPI_INT, 0, MPIMessage::BARRIER_JOIN);
    }

    // Rank 0 broadcasts that the barrier is done (the others block here)
    broadcast(0, thisRank, nullptr, MPI_INT, 0, MPIMessage::BARRIER_DONE);
    SPDLOG_TRACE("MPI - barrier done {}", thisRank);
}

std::shared_ptr<InMemoryMpiQueue> MpiWorld::getLocalQueue(int sendRank,
                                                          int recvRank)
{
    assert(getHostForRank(recvRank) == thisHost);
    assert(localQueues.size() == size * size);

    return localQueues[getIndexForRanks(sendRank, recvRank)];
}

// We pre-allocate all _potentially_ necessary queues in advance. Queues are
// necessary to _receive_ messages, thus we initialise all queues whose
// corresponding receiver is local to this host
// Note - the queues themselves perform concurrency control
void MpiWorld::initLocalQueues()
{
    localQueues.resize(size * size);
    for (const int sendRank : ranksForHost[thisHost]) {
        for (const int recvRank : ranksForHost[thisHost]) {
            if (localQueues[getIndexForRanks(sendRank, recvRank)] == nullptr) {
                localQueues[getIndexForRanks(sendRank, recvRank)] =
                  std::make_shared<InMemoryMpiQueue>();
            }
        }
    }
}

std::shared_ptr<MPIMessage> MpiWorld::recvBatchReturnLast(int sendRank,
                                                          int recvRank,
                                                          int batchSize)
{
    std::shared_ptr<MpiMessageBuffer> umb =
      getUnackedMessageBuffer(sendRank, recvRank);

    // When calling from recv, we set the batch size to zero and work
    // out the total here. We want to acknowledge _all_ unacknowleged messages
    // _and then_ receive ours (which is not in the MMB).
    if (batchSize == 0) {
        batchSize = umb->getTotalUnackedMessages() + 1;
    }

    // Work out whether the message is sent locally or from another host
    assert(thisHost == getHostForRank(recvRank));
    const std::string otherHost = getHostForRank(sendRank);
    bool isLocal = otherHost == thisHost;

    // Recv message: first we receive all messages for which there is an id
    // in the unacknowleged buffer but no msg. Note that these messages
    // (batchSize - 1) were `irecv`-ed before ours.
    std::shared_ptr<MPIMessage> ourMsg;
    auto msgIt = umb->getFirstNullMsg();
    if (isLocal) {
        // First receive messages that happened before us
        for (int i = 0; i < batchSize - 1; i++) {
            try {
                SPDLOG_TRACE("MPI - pending recv {} -> {}", sendRank, recvRank);
                auto pendingMsg = getLocalQueue(sendRank, recvRank)->dequeue();

                // Put the unacked message in the UMB
                assert(!msgIt->isAcknowledged());
                msgIt->acknowledge(pendingMsg);
                msgIt++;
            } catch (faabric::util::QueueTimeoutException& e) {
                SPDLOG_ERROR(
                  "{}:{}:{} Timed out with: MPI - pending recv {} -> {}",
                  thisRankMsg->appid(),
                  thisRankMsg->groupid(),
                  thisRankMsg->groupidx(),
                  sendRank,
                  recvRank);
                throw e;
            }
        }

        // Finally receive the message corresponding to us
        SPDLOG_TRACE("MPI - recv {} -> {}", sendRank, recvRank);
        try {
            ourMsg = getLocalQueue(sendRank, recvRank)->dequeue();
        } catch (faabric::util::QueueTimeoutException& e) {
            SPDLOG_ERROR("{}:{}:{} Timed out with: MPI - recv {} -> {}",
                         thisRankMsg->appid(),
                         thisRankMsg->groupid(),
                         thisRankMsg->groupidx(),
                         sendRank,
                         recvRank);
            throw e;
        }
    } else {
        // First receive messages that happened before us
        for (int i = 0; i < batchSize - 1; i++) {
            SPDLOG_TRACE(
              "MPI - pending remote recv {} -> {}", sendRank, recvRank);
            auto pendingMsg = recvRemoteMpiMessage(sendRank, recvRank);

            // Put the unacked message in the UMB
            assert(!msgIt->isAcknowledged());
            msgIt->acknowledge(pendingMsg);
            msgIt++;
        }

        // Finally receive the message corresponding to us
        SPDLOG_TRACE("MPI - recv remote {} -> {}", sendRank, recvRank);
        ourMsg = recvRemoteMpiMessage(sendRank, recvRank);
    }

    return ourMsg;
}

int MpiWorld::getIndexForRanks(int sendRank, int recvRank) const
{
    int index = sendRank * size + recvRank;
    assert(index >= 0 && index < size * size);
    return index;
}

long MpiWorld::getLocalQueueSize(int sendRank, int recvRank)
{
    const std::shared_ptr<InMemoryMpiQueue>& queue =
      getLocalQueue(sendRank, recvRank);
    return queue->size();
}

double MpiWorld::getWTime()
{
    double t = faabric::util::getTimeDiffMillis(creationTime);
    return t / 1000.0;
}

std::vector<bool> MpiWorld::getInitedUMB()
{
    std::vector<bool> retVec(unackedMessageBuffers.size());
    for (int i = 0; i < unackedMessageBuffers.size(); i++) {
        retVec.at(i) = unackedMessageBuffers.at(i) != nullptr;
    }

    return retVec;
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

void MpiWorld::prepareMigration(int thisRank)
{
    // Check that there are no pending asynchronous messages to send and receive
    for (auto umb : unackedMessageBuffers) {
        if (umb != nullptr && umb->size() > 0) {
            SPDLOG_ERROR("Trying to migrate MPI application (id: {}) but rank"
                         " {} has {} pending async messages to receive",
                         thisRankMsg->appid(),
                         thisRank,
                         umb->size());
            throw std::runtime_error(
              "Migrating with pending async messages is not supported");
        }
    }

    if (!iSendRequests.empty()) {
        SPDLOG_ERROR("Trying to migrate MPI application (id: {}) but rank"
                     " {} has {} pending async send messages to acknowledge",
                     thisRankMsg->appid(),
                     thisRank,
                     iSendRequests.size());
        throw std::runtime_error(
          "Migrating with pending async messages is not supported");
    }

    // Update local records
    if (thisRank == localLeader) {
        initLocalRemoteLeaders();

        // Add the necessary new local messaging queues
        initLocalQueues();
    }
}
}
