from subprocess import check_output

HOME = "/home/csegarra"
GHZ_DIR = "{}/sof/ghz".format(HOME)
FAABRIC_DIR = "{}/faasm/faabric".format(HOME)
PROTO_FILE = "{}/src/proto/faabric.proto".format(FAABRIC_DIR)
PROTO_INCLUDE = "{}/include/faabric/proto".format(FAABRIC_DIR)

RPC_CALL = "faabric.FunctionRPCService.NoOp"
RPC_IP = "172.29.0.3"
RPC_PORT = "8004"


def clientTest(numThreads, numRpcs):
    _cmd = [
        "/home/csegarra/sof/ghz",
        "--proto {}".format(PROTO_FILE),
        "-i {}".format(PROTO_INCLUDE),
        "--call {}".format(RPC_CALL),
        "--insecure",
        "-n {}".format(numRpcs),
        "--cpus {}".format(numThreads),
        "{}:{}".format(RPC_IP, RPC_PORT)
    ]

    cmd = " ".join(_cmd)
    print(cmd)
    out = check_output(cmd, shell=True).decode("utf-8")

    print("{} clients and {} RPC/client".format(numThreads, numRpcs))
    print(out)


def benchmark():
    numThreads = [1, 2, 4, 8]
    numRpc = [8000, 8000, 8000, 8000]
    for c, r in zip(numThreads, numRpc):
        clientTest(c, r)


if __name__ == "__main__":
    benchmark()

