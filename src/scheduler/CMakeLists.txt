file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/scheduler/*.h")

set(LIB_FILES
        ExecutorFactory.cpp
        Executor.cpp
        ExecGraph.cpp
        FunctionCallClient.cpp
        FunctionCallServer.cpp
        Scheduler.cpp
        SnapshotServer.cpp
        SnapshotClient.cpp
        MpiContext.cpp
        MpiMessageBuffer.cpp
        MpiWorldRegistry.cpp
        MpiWorld.cpp
        ${HEADERS}
        )

faabric_lib(scheduler "${LIB_FILES}")

target_link_libraries(scheduler flat proto snapshot state faabricmpi redis transport)
