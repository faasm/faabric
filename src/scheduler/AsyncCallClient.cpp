#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/AsyncCallClient.h>

#include <grpcpp/grpcpp.h>

#include <faabric/util/logging.h>

namespace faabric::scheduler {
// -----------------------------------
// gRPC asynchronous client
// -----------------------------------
AsyncCallClient::AsyncCallClient(const std::string& hostIn)
  : host(hostIn)
  , channel(grpc::CreateChannel(host + ":" + std::to_string(ASYNC_FUNCTION_CALL_PORT),
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
    // Message we are sending
    faabric::MPIMessage m = *msg;

    // Response we are receiving
    faabric::FunctionStatusResponse response;

    // Prepare call. Note that this does not actually start the RPC.
    AsyncClientCall* call = new AsyncClientCall;
    call->response_reader = stub->PrepareAsyncMPIMsg(&call->context, m, &cq);

    // Initiate RPC
    call->response_reader->StartCall();

    // Wait for responses in a separate loop to make the sending fully async
    call->response_reader->Finish(&call->response, &call->status, (void*) call);
}

// RPC response reader to be spawned in a separate thread
void AsyncClientCall::AsyncCompleteRpc() {
    void* gotTag;
    bool ok = false;

    while(cq.Next(&gotTag, &ok)) {
        AsyncClientCall* call = static_cast<AsyncClientCall*>(gotTag);

        // Check that the request completed succesfully. Note that this does not
        // check the status code, only that it finished.
        if (!ok) {
            throw std::runtime_error("Async RPC did not finish succesfully");
        }

        if (!call->status.ok()) {
            throw std::runtime_error(fmt::format("RPC error {}", call->status.error_message()));
        }
    }
}
}
