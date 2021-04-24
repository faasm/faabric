#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/macros.h>
#include <faabric/scheduler/AsyncCallClient.h>

#include <grpcpp/grpcpp.h>

#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

namespace faabric::scheduler {
// -----------------------------------
// gRPC asynchronous client
// -----------------------------------
AsyncCallClient::AsyncCallClient(const std::string& hostIn)
  : host(hostIn)
  , channel(
      grpc::CreateChannel(host + ":" + std::to_string(ASYNC_FUNCTION_CALL_PORT),
                          grpc::InsecureChannelCredentials()))
  , stub(faabric::AsyncRPCService::NewStub(channel))
{
    // TODO remove
    auto logger = faabric::util::getLogger();
    logger->warn("Created a new async RPC client.");
}

AsyncCallClient::~AsyncCallClient()
{
    this->doShutdown();
}

void AsyncCallClient::doShutdown()
{
    faabric::util::getLogger()->warn("AsyncCallClient dtor");

    // Send shutdown message to ourselves to kill the response reader thread
    this->sendMpiMessage(nullptr);
    if (this->responseThread.joinable()) {
        this->responseThread.join();
    }
}

void AsyncCallClient::sendMpiMessage(
  const std::shared_ptr<faabric::MPIMessage> msg)
{
    // Check shutdown
    // Note - we don't bother draining the queue during shutdown
    if (!msg) {
        faabric::util::getLogger()->debug("Shutting down RPC response queue");
        cq.Shutdown();
    } else {
        PROF_START(asyncSend)
        // Message we are sending
        faabric::MPIMessage m = *msg;

        // Response we are receiving
        faabric::FunctionStatusResponse response;

        // Prepare call. Note that this does not actually start the RPC.
        AsyncCall* call = new AsyncCall;
        call->responseReader = stub->AsyncMPIMsg(&call->context, m, &cq);

        // Wait for responses in a separate loop to make the sending fully async
        call->responseReader->Finish(&call->response, &call->status, call);
        PROF_END(asyncSend)
    }
}

// This method consumes the async requests that are already finished and checks
// that the status is OK
// Note: this method should run in a separate thread for a true asynchronous
// behaviour
void AsyncCallClient::AsyncCompleteRpc()
{
    while (true) {
        void* gotTag;
        bool ok = false;
        if (!cq.Next(&gotTag, &ok)) {
            break;
        }
        AsyncCall* call = static_cast<AsyncCall*>(gotTag);

        // Check that the request completed succesfully. Note that this does not
        // check the status code, only that it finished.
        if (!ok) {
            throw std::runtime_error("Async RPC did not finish succesfully");
        }

        // Check that the processing of the RPC was OK
        if (!call->status.ok()) {
            throw std::runtime_error(
              fmt::format("RPC error {}", call->status.error_message()));
        }
        delete call;
    }

    faabric::util::getLogger()->debug("Exiting AsyncComplteRpc");
}

void AsyncCallClient::startResponseReaderThread()
{
    this->responseThread =
      std::thread(&faabric::scheduler::AsyncCallClient::AsyncCompleteRpc, this);
}
}
