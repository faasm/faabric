#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/macros.h>

/* Each MPI rank runs in a separate thread, thus we use TLS to maintain the
 * per-rank data structures.
 */
static thread_local std::vector<
  std::unique_ptr<faabric::transport::MpiMessageEndpoint>>
  mpiMessageEndpoints;
static thread_local std::vector<
  std::shared_ptr<faabric::scheduler::MpiMessageBuffer>>
  unackedMessageBuffers;
static thread_local std::set<int> iSendRequests;
static thread_local std::map<int, std::pair<int, int>> reqIdToRanks;

namespace faabric::scheduler {
MpiWorld::MpiWorld()
  : id(-1)
  , size(-1)
  , thisHost(faabric::util::getSystemConfig().endpointHost)
  , creationTime(faabric::util::startTimer())
  , cartProcsPerDim(2)
{}

void MpiWorld::initRemoteMpiEndpoint(int localRank, int remoteRank)
{
    SPDLOG_TRACE("Open MPI endpoint between ranks (local-remote) {} - {}",
                 localRank,
                 remoteRank);

    // Resize the message endpoint vector and initialise to null. Note that we
    // allocate size x size slots to cover all possible (sendRank, recvRank)
    // pairs
    if (mpiMessageEndpoints.empty()) {
        for (int i = 0; i < size * size; i++) {
            mpiMessageEndpoints.emplace_back(nullptr);
        }
    }

    // Get host for remote rank
    std::string otherHost = getHostForRank(remoteRank);

    // Get the index for the rank-host pair
    int index = getIndexForRanks(localRank, remoteRank);

    // Get port for send-recv pair
    std::pair<int, int> sendRecvPorts = getPortForRanks(localRank, remoteRank);

    // Create MPI message endpoint
    mpiMessageEndpoints.emplace(
      mpiMessageEndpoints.begin() + index,
      std::make_unique<faabric::transport::MpiMessageEndpoint>(
        otherHost, sendRecvPorts.first, sendRecvPorts.second));
}

void MpiWorld::sendRemoteMpiMessage(
  int sendRank,
  int recvRank,
  const std::shared_ptr<faabric::MPIMessage>& msg)
{
    // Get the index for the rank-host pair
    // Note - message endpoints are identified by a (localRank, remoteRank)
    // pair, not a (sendRank, recvRank) one
    int index = getIndexForRanks(sendRank, recvRank);

    if (mpiMessageEndpoints.empty() || mpiMessageEndpoints[index] == nullptr) {
        initRemoteMpiEndpoint(sendRank, recvRank);
    }

    mpiMessageEndpoints[index]->sendMpiMessage(msg);
}

std::shared_ptr<faabric::MPIMessage> MpiWorld::recvRemoteMpiMessage(
  int sendRank,
  int recvRank)
{
    // Get the index for the rank-host pair
    // Note - message endpoints are identified by a (localRank, remoteRank)
    // pair, not a (sendRank, recvRank) one
    int index = getIndexForRanks(recvRank, sendRank);

    if (mpiMessageEndpoints.empty() || mpiMessageEndpoints[index] == nullptr) {
        initRemoteMpiEndpoint(recvRank, sendRank);
    }

    return mpiMessageEndpoints[index]->recvMpiMessage();
}

std::shared_ptr<faabric::scheduler::MpiMessageBuffer>
MpiWorld::getUnackedMessageBuffer(int sendRank, int recvRank)
{
    // We want to lazily initialise this data structure because, given its
    // thread local nature, we expect it to be quite sparse (i.e. filled with
    // nullptr).
    if (unackedMessageBuffers.size() == 0) {
        unackedMessageBuffers.resize(size * size, nullptr);
    }

    // Get the index for the rank-host pair
    int index = getIndexForRanks(sendRank, recvRank);
    assert(index >= 0 && index < size * size);

    if (unackedMessageBuffers[index] == nullptr) {
        unackedMessageBuffers.emplace(
          unackedMessageBuffers.begin() + index,
          std::make_shared<faabric::scheduler::MpiMessageBuffer>());
    }

    return unackedMessageBuffers[index];
}

void MpiWorld::create(const faabric::Message& call, int newId, int newSize)
{
    id = newId;
    user = call.user();
    function = call.function();

    size = newSize;

    auto& sch = faabric::scheduler::getScheduler();

    // Dispatch all the chained calls
    // NOTE - with the master being rank zero, we want to spawn
    // (size - 1) new functions starting with rank 1
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory(user, function, size - 1);
    for (int i = 0; i < req->messages_size(); i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        msg.set_ismpi(true);
        msg.set_mpiworldid(id);
        msg.set_mpirank(i + 1);
        msg.set_mpiworldsize(size);
    }

    std::vector<std::string> executedAt;
    if (size > 1) {
        // Send the init messages (note that message i corresponds to rank i+1)
        executedAt = sch.callFunctions(req);
    }
    assert(executedAt.size() == size - 1);

    // Prepend this host for rank 0
    executedAt.insert(executedAt.begin(), thisHost);

    // Register hosts to rank mappings on this host
    faabric::MpiHostsToRanksMessage hostRankMsg;
    *hostRankMsg.mutable_hosts() = { executedAt.begin(), executedAt.end() };

    // Prepare the base port for each rank
    std::vector<int> basePortForRank = initLocalBasePorts(executedAt);
    *hostRankMsg.mutable_baseports() = { basePortForRank.begin(),
                                         basePortForRank.end() };

    // Register hosts to rank mappins on this host
    setAllRankHostsPorts(hostRankMsg);

    // Set up a list of hosts to broadcast to (excluding this host)
    std::set<std::string> hosts(executedAt.begin(), executedAt.end());
    hosts.erase(thisHost);

    // Do the broadcast
    for (const auto& h : hosts) {
        faabric::transport::sendMpiHostRankMsg(h, hostRankMsg);
    }

    // Initialise the memory queues for message reception
    initLocalQueues();
}

void MpiWorld::destroy()
{
    // Destroy once per thread the rank-specific data structures
    // Remote message endpoints
    if (!mpiMessageEndpoints.empty()) {
        for (auto& e : mpiMessageEndpoints) {
            if (e != nullptr) {
                e->close();
            }
        }
        mpiMessageEndpoints.clear();
    }

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
}

void MpiWorld::initialiseFromMsg(const faabric::Message& msg, bool forceLocal)
{
    id = msg.mpiworldid();
    user = msg.user();
    function = msg.function();
    size = msg.mpiworldsize();

    // Sometimes for testing purposes we may want to initialise a world in the
    // _same_ host we have created one (note that this would never happen in
    // reality). If so, we skip initialising resources already initialised
    if (!forceLocal) {
        // Block until we receive
        faabric::MpiHostsToRanksMessage hostRankMsg =
          faabric::transport::recvMpiHostRankMsg();
        setAllRankHostsPorts(hostRankMsg);

        // Initialise the memory queues for message reception
        initLocalQueues();
    }
}

std::string MpiWorld::getHostForRank(int rank)
{
    assert(rankHosts.size() == size);

    std::string host = rankHosts[rank];
    if (host.empty()) {
        throw std::runtime_error(
          fmt::format("No host found for rank {}", rank));
    }

    return host;
}

// Returns a pair (sendPort, recvPort)
// To assign the send and recv ports, we follow a protocol establishing:
// 1) Port range (offset) corresponding to the world that receives
// 2) Within a world's port range, port corresponding to the outcome of
//    getIndexForRanks(localRank, remoteRank) Where local and remote are
//    relative to the world whose port range we are in
std::pair<int, int> MpiWorld::getPortForRanks(int localRank, int remoteRank)
{
    std::pair<int, int> sendRecvPortPair;

    // Get base port for local and remote worlds
    int localBasePort = basePorts[localRank];
    int remoteBasePort = basePorts[remoteRank];
    assert(localBasePort != remoteBasePort);

    // Assign send port
    // 1) Port range corresponding to remote world, as they are receiving
    // 2) Index switching localRank and remoteRank, as remote rank is "local"
    //    to the remote world
    sendRecvPortPair.first =
      remoteBasePort + getIndexForRanks(remoteRank, localRank);

    // Assign recv port
    // 1) Port range corresponding to our world, as we are the one's receiving
    // 2) Port using our local rank as `localRank`, as we are in the local
    //    offset
    sendRecvPortPair.second =
      localBasePort + getIndexForRanks(localRank, remoteRank);

    return sendRecvPortPair;
}

// Prepare the host-rank map with a vector containing _all_ ranks
// Note - this method should be called by only one rank. This is enforced in
// the world registry
void MpiWorld::setAllRankHostsPorts(const faabric::MpiHostsToRanksMessage& msg)
{
    // Assert we are only setting the values once
    assert(rankHosts.size() == 0);
    assert(basePorts.size() == 0);

    assert(msg.hosts().size() == size);
    assert(msg.baseports().size() == size);
    rankHosts = { msg.hosts().begin(), msg.hosts().end() };
    basePorts = { msg.baseports().begin(), msg.baseports().end() };
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
                    faabric::MPIMessage::MPIMessageType messageType)
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
                    faabric::MPIMessage::MPIMessageType messageType)
{
    int requestId = (int)faabric::util::generateGid();
    reqIdToRanks.try_emplace(requestId, sendRank, recvRank);

    // Enqueue an unacknowleged request (no message)
    faabric::scheduler::MpiMessageBuffer::PendingAsyncMpiMessage pendingMsg;
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
                    faabric::MPIMessage::MPIMessageType messageType)
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
    int msgId = (int)faabric::util::generateGid();

    // Create the message
    auto m = std::make_shared<faabric::MPIMessage>();
    m->set_id(msgId);
    m->set_worldid(id);
    m->set_sender(sendRank);
    m->set_destination(recvRank);
    m->set_type(dataType->id);
    m->set_count(count);
    m->set_messagetype(messageType);

    // Set up message data
    if (count > 0 && buffer != nullptr) {
        m->set_buffer(buffer, dataType->size * count);
    }

    // Dispatch the message locally or globally
    if (isLocal) {
        SPDLOG_TRACE("MPI - send {} -> {}", sendRank, recvRank);
        getLocalQueue(sendRank, recvRank)->enqueue(std::move(m));
    } else {
        SPDLOG_TRACE("MPI - send remote {} -> {}", sendRank, recvRank);
        sendRemoteMpiMessage(sendRank, recvRank, m);
    }
}

void MpiWorld::recv(int sendRank,
                    int recvRank,
                    uint8_t* buffer,
                    faabric_datatype_t* dataType,
                    int count,
                    MPI_Status* status,
                    faabric::MPIMessage::MPIMessageType messageType)
{
    // Sanity-check input parameters
    checkRanksRange(sendRank, recvRank);

    // Recv message from underlying transport
    std::shared_ptr<faabric::MPIMessage> m =
      recvBatchReturnLast(sendRank, recvRank);

    // Do the processing
    doRecv(m, buffer, dataType, count, status, messageType);
}

void MpiWorld::doRecv(std::shared_ptr<faabric::MPIMessage> m,
                      uint8_t* buffer,
                      faabric_datatype_t* dataType,
                      int count,
                      MPI_Status* status,
                      faabric::MPIMessage::MPIMessageType messageType)
{
    // Assert message integrity
    // Note - this checks won't happen in Release builds
    assert(m->messagetype() == messageType);
    assert(m->count() <= count);

    // TODO - avoid copy here
    // Copy message data
    if (m->count() > 0) {
        std::move(m->buffer().begin(), m->buffer().end(), buffer);
    }

    // Set status values if required
    if (status != nullptr) {
        status->MPI_SOURCE = m->sender();
        status->MPI_ERROR = MPI_SUCCESS;

        // Note, take the message size here as the receive count may be larger
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
                       faabric::MPIMessage::SENDRECV);
    // Then send the message
    send(myRank,
         sendRank,
         sendBuffer,
         sendDataType,
         sendCount,
         faabric::MPIMessage::SENDRECV);
    // And wait
    awaitAsyncRequest(recvId);
}

void MpiWorld::broadcast(int sendRank,
                         const uint8_t* buffer,
                         faabric_datatype_t* dataType,
                         int count,
                         faabric::MPIMessage::MPIMessageType messageType)
{
    SPDLOG_TRACE("MPI - bcast {} -> all", sendRank);

    for (int r = 0; r < size; r++) {
        // Skip this rank (it's doing the broadcasting)
        if (r == sendRank) {
            continue;
        }

        // Send to the other ranks
        send(sendRank, r, buffer, dataType, count, messageType);
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
                     faabric::MPIMessage::SCATTER);
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
             faabric::MPIMessage::SCATTER);
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

    size_t sendOffset = sendCount * sendType->size;
    size_t recvOffset = recvCount * recvType->size;

    bool isInPlace = sendBuffer == recvBuffer;

    // If we're the root, do the gathering
    if (sendRank == recvRank) {
        SPDLOG_TRACE("MPI - gather all -> {}", recvRank);

        // Iterate through each rank
        for (int r = 0; r < size; r++) {
            // Work out where in the receive buffer this rank's data goes
            uint8_t* recvChunk = recvBuffer + (r * recvOffset);

            if ((r == recvRank) && isInPlace) {
                // If operating in-place, data for the root rank is already in
                // position
                continue;
            } else if (r == recvRank) {
                // Copy data locally on root
                std::copy(sendBuffer, sendBuffer + sendOffset, recvChunk);
            } else {
                // Receive data from rank if it's not the root
                recv(r,
                     recvRank,
                     recvChunk,
                     recvType,
                     recvCount,
                     nullptr,
                     faabric::MPIMessage::GATHER);
            }
        }
    } else {
        if (isInPlace) {
            // A non-root rank running gather "in place" happens as part of an
            // allgather operation. In this case, the send and receive buffer
            // are the same, and the rank is eventually expecting a broadcast of
            // the gather result into this buffer. This means that this buffer
            // is big enough for the whole gather result, with this rank's data
            // already in place. Therefore we need to send _only_ the part of
            // the send buffer relating to this rank.
            const uint8_t* sendChunk = sendBuffer + (sendRank * sendOffset);
            send(sendRank,
                 recvRank,
                 sendChunk,
                 sendType,
                 sendCount,
                 faabric::MPIMessage::GATHER);
        } else {
            // Normal sending
            send(sendRank,
                 recvRank,
                 sendBuffer,
                 sendType,
                 sendCount,
                 faabric::MPIMessage::GATHER);
        }
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
    if (rank == root) {
        // Broadcast the result
        broadcast(root,
                  recvBuffer,
                  recvType,
                  fullCount,
                  faabric::MPIMessage::ALLGATHER);
    } else {
        // Await the broadcast from the master
        recv(root,
             rank,
             recvBuffer,
             recvType,
             fullCount,
             nullptr,
             faabric::MPIMessage::ALLGATHER);
    }
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

    std::shared_ptr<faabric::scheduler::MpiMessageBuffer> umb =
      getUnackedMessageBuffer(sendRank, recvRank);

    std::list<MpiMessageBuffer::PendingAsyncMpiMessage>::iterator msgIt =
      umb->getRequestPendingMsg(requestId);

    std::shared_ptr<faabric::MPIMessage> m;
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
    // If we're the receiver, await inputs
    if (sendRank == recvRank) {
        SPDLOG_TRACE("MPI - reduce ({}) all -> {}", operation->id, recvRank);

        size_t bufferSize = datatype->size * count;

        bool isInPlace = sendBuffer == recvBuffer;

        // If not receiving in-place, initialize the receive buffer to the send
        // buffer values. This prevents issues when 0-initializing for operators
        // like the minimum, or product.
        // If we're receiving from ourselves and in-place, our work is
        // already done and the results are written in the recv buffer
        if (!isInPlace) {
            memcpy(recvBuffer, sendBuffer, bufferSize);
        }

        uint8_t* rankData = new uint8_t[bufferSize];
        for (int r = 0; r < size; r++) {
            // Work out the data for this rank
            memset(rankData, 0, bufferSize);
            if (r != recvRank) {
                recv(r,
                     recvRank,
                     rankData,
                     datatype,
                     count,
                     nullptr,
                     faabric::MPIMessage::REDUCE);

                op_reduce(operation, datatype, count, rankData, recvBuffer);
            }
        }

        delete[] rankData;

    } else {
        // Do the sending
        send(sendRank,
             recvRank,
             sendBuffer,
             datatype,
             count,
             faabric::MPIMessage::REDUCE);
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
    if (rank == 0) {
        // Run the standard reduce
        reduce(0, 0, sendBuffer, recvBuffer, datatype, count, operation);

        // Broadcast the result
        broadcast(
          0, recvBuffer, datatype, count, faabric::MPIMessage::ALLREDUCE);
    } else {
        // Run the standard reduce
        reduce(rank, 0, sendBuffer, recvBuffer, datatype, count, operation);

        // Await the broadcast from the master
        recv(0,
             rank,
             recvBuffer,
             datatype,
             count,
             nullptr,
             faabric::MPIMessage::ALLREDUCE);
    }
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
        memcpy(recvBuffer, sendBuffer, bufferSize);
    }

    if (rank > 0) {
        // Receive the current accumulated value
        auto currentAcc = new uint8_t[bufferSize];
        recv(rank - 1,
             rank,
             currentAcc,
             datatype,
             count,
             nullptr,
             faabric::MPIMessage::SCAN);
        // Reduce with our own value
        op_reduce(operation, datatype, count, currentAcc, recvBuffer);
        delete[] currentAcc;
    }

    // If not the last process, send to the next one
    if (rank < this->size - 1) {
        send(rank,
             rank + 1,
             recvBuffer,
             MPI_INT,
             count,
             faabric::MPIMessage::SCAN);
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
            send(rank,
                 r,
                 sendChunk,
                 sendType,
                 sendCount,
                 faabric::MPIMessage::ALLTOALL);
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
             faabric::MPIMessage::ALLTOALL);
    }
}

void MpiWorld::probe(int sendRank, int recvRank, MPI_Status* status)
{
    const std::shared_ptr<InMemoryMpiQueue>& queue =
      getLocalQueue(sendRank, recvRank);
    std::shared_ptr<faabric::MPIMessage> m = *(queue->peek());

    faabric_datatype_t* datatype = getFaabricDatatypeFromId(m->type());
    status->bytesSize = m->count() * datatype->size;
    status->MPI_ERROR = 0;
    status->MPI_SOURCE = m->sender();
}

void MpiWorld::barrier(int thisRank)
{
    if (thisRank == 0) {
        // This is the root, hence just does the waiting
        SPDLOG_TRACE("MPI - barrier init {}", thisRank);

        // Await messages from all others
        for (int r = 1; r < size; r++) {
            MPI_Status s{};
            recv(
              r, 0, nullptr, MPI_INT, 0, &s, faabric::MPIMessage::BARRIER_JOIN);
            SPDLOG_TRACE("MPI - recv barrier join {}", s.MPI_SOURCE);
        }

        // Broadcast that the barrier is done
        broadcast(0, nullptr, MPI_INT, 0, faabric::MPIMessage::BARRIER_DONE);
    } else {
        // Tell the root that we're waiting
        SPDLOG_TRACE("MPI - barrier join {}", thisRank);
        send(
          thisRank, 0, nullptr, MPI_INT, 0, faabric::MPIMessage::BARRIER_JOIN);

        // Receive a message saying the barrier is done
        recv(0,
             thisRank,
             nullptr,
             MPI_INT,
             0,
             nullptr,
             faabric::MPIMessage::BARRIER_DONE);
        SPDLOG_TRACE("MPI - barrier done {}", thisRank);
    }
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
    // Assert we only allocate queues once
    assert(localQueues.size() == 0);
    localQueues.resize(size * size);
    for (int recvRank = 0; recvRank < size; recvRank++) {
        if (getHostForRank(recvRank) == thisHost) {
            for (int sendRank = 0; sendRank < size; sendRank++) {
                localQueues[getIndexForRanks(sendRank, recvRank)] =
                  std::make_shared<InMemoryMpiQueue>();
            }
        }
    }
}

// Here we rely on the scheduler returning a list of hosts where equal
// hosts are always contiguous with the exception of the master host
// (thisHost) which may appear repeated at the end if the system is
// overloaded.
std::vector<int> MpiWorld::initLocalBasePorts(
  const std::vector<std::string>& executedAt)
{
    std::vector<int> basePortForRank;
    basePortForRank.reserve(size);

    std::string lastHost = thisHost;
    int lastPort = MPI_PORT;
    for (const auto& host : executedAt) {
        if (host == thisHost) {
            basePortForRank.push_back(MPI_PORT);
        } else if (host == lastHost) {
            basePortForRank.push_back(lastPort);
        } else {
            lastHost = host;
            lastPort += size * size;
            basePortForRank.push_back(lastPort);
        }
    }

    assert(basePortForRank.size() == size);
    return basePortForRank;
}

std::shared_ptr<faabric::MPIMessage>
MpiWorld::recvBatchReturnLast(int sendRank, int recvRank, int batchSize)
{
    std::shared_ptr<faabric::scheduler::MpiMessageBuffer> umb =
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
    std::shared_ptr<faabric::MPIMessage> ourMsg;
    auto msgIt = umb->getFirstNullMsg();
    if (isLocal) {
        // First receive messages that happened before us
        for (int i = 0; i < batchSize - 1; i++) {
            SPDLOG_TRACE("MPI - pending recv {} -> {}", sendRank, recvRank);
            auto pendingMsg = getLocalQueue(sendRank, recvRank)->dequeue();

            // Put the unacked message in the UMB
            assert(!msgIt->isAcknowledged());
            msgIt->acknowledge(pendingMsg);
            msgIt++;
        }

        // Finally receive the message corresponding to us
        SPDLOG_TRACE("MPI - recv {} -> {}", sendRank, recvRank);
        ourMsg = getLocalQueue(sendRank, recvRank)->dequeue();
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

int MpiWorld::getIndexForRanks(int sendRank, int recvRank)
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

std::string MpiWorld::getUser()
{
    return user;
}

std::string MpiWorld::getFunction()
{
    return function;
}

int MpiWorld::getId()
{
    return id;
}

int MpiWorld::getSize()
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
}
