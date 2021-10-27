
faabric_lib(scheduler
    ExecutorFactory.cpp
    Executor.cpp
    ExecGraph.cpp
    FunctionCallClient.cpp
    FunctionCallServer.cpp
    Scheduler.cpp
    MpiContext.cpp
    MpiMessageBuffer.cpp
    MpiWorldRegistry.cpp
    MpiWorld.cpp
)

target_link_libraries(scheduler PRIVATE
    faabric::snapshot
    faabric::state
    faabric::faabricmpi
    faabric::redis
)
