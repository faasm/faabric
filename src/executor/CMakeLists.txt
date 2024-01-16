faabric_lib(executor
    Executor.cpp
    ExecutorContext.cpp
    ExecutorFactory.cpp
    ExecutorTask.cpp
)

# FIXME: do we need all these deps here?
target_link_libraries(executor PRIVATE
    faabric::scheduling_util
    faabric::snapshot
    faabric::state
    faabric::redis
)
