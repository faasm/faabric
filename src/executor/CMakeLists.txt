include_directories(${FAABRIC_INCLUDE_DIR}/faabric/executor)

file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/executor/*.h")

set(LIB_FILES
        FaabricExecutor.cpp
        FaabricMain.cpp
        FaabricPool.cpp
        faabric_runner.cpp
        ${HEADERS}
        )

faabric_private_lib(executor "${LIB_FILES}")

target_link_libraries(executor scheduler)
