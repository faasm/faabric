#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/macros.h>
#include <faabric/scheduler/AsyncCallClient.h>

#include <grpcpp/grpcpp.h>

#include <faabric/util/logging.h>

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

void AsyncCallClient::sendMpiMessage(
  const std::shared_ptr<faabric::MPIMessage> msg)
{
    // Check shutdown
    if (!msg) {
        faabric::util::getLogger()->info("Shutting down queue");
        cq.Shutdown();
    } else {
        // Message we are sending
        faabric::MPIMessage m = *msg;

        // Response we are receiving
        faabric::FunctionStatusResponse response;

        // Prepare call. Note that this does not actually start the RPC.
        AsyncCall* call = new AsyncCall;
        call->response_reader =
          stub->PrepareAsyncMPIMsg(&call->context, m, &cq);

        // Initiate RPC
        call->response_reader->StartCall();

        // Wait for responses in a separate loop to make the sending fully async
        call->response_reader->Finish(
          &call->response, &call->status, (void*)call);
    }
}

// This method consumes the async requests that are already finished and checks
// that the status is OK
// Note: this method should run in a separate thread for a true asynchronous
// behaviour
void AsyncCallClient::AsyncCompleteRpc()
{
    void* gotTag;
    bool ok = false;

    while (cq.Next(&gotTag, &ok)) {
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
    }

    faabric::util::getLogger()->info("Exiting AsyncComplteRpc");
}
}
