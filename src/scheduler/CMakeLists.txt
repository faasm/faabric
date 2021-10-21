file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/scheduler/*.h")

set(LIB_FILES
        DistributedCoordinationGroup.cpp
        DistributedCoordinator.cpp
        ExecutorFactory.cpp
        Executor.cpp
        ExecGraph.cpp
        FunctionCallClient.cpp
        FunctionCallServer.cpp
        MpiContext.cpp
        MpiMessageBuffer.cpp
        MpiWorldRegistry.cpp
        MpiWorld.cpp
        Scheduler.cpp
        ${HEADERS}
        )

faabric_lib(scheduler "${LIB_FILES}")

target_link_libraries(scheduler snapshot state faabricmpi redis)
