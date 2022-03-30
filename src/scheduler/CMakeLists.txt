faabric_lib(scheduler
    ExecGraph.cpp
    ExecutorContext.cpp
    ExecutorFactory.cpp
    Executor.cpp
    FunctionCallClient.cpp
    FunctionCallServer.cpp
    MpiContext.cpp
    MpiMessageBuffer.cpp
    MpiWorld.cpp
    MpiWorldRegistry.cpp
    Scheduler.cpp
)

target_link_libraries(scheduler PRIVATE
    faabric::snapshot
    faabric::state
    faabric::faabricmpi
    faabric::redis
)
