faabric_lib(scheduler
    ExecGraph.cpp
    ExecutorContext.cpp
    ExecutorFactory.cpp
    Executor.cpp
    FunctionCallClient.cpp
    FunctionCallServer.cpp
    Scheduler.cpp
)

target_link_libraries(scheduler PRIVATE
    faabric::snapshot
    faabric::state
    faabric::redis
)
