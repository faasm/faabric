faabric_lib(scheduler
    ExecutorContext.cpp
    ExecutorFactory.cpp
    Executor.cpp
    FunctionCallClient.cpp
    FunctionCallServer.cpp
    Scheduler.cpp
)

target_link_libraries(scheduler PRIVATE
    faabric::scheduling_util
    faabric::snapshot
    faabric::state
    faabric::redis
)
