include_directories(${FAABRIC_INCLUDE_DIR}/faabric/executor)

file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/executor/*.h")

set(LIB_FILES
        FaabricExecutor.cpp
        FaabricMain.cpp
        FaabricPool.cpp
        ${HEADERS}
        )

faabric_private_lib(executor "${LIB_FILES}")

target_link_libraries(executor scheduler)

add_executable(faabric_runner faabric_runner.cpp)

target_link_libraries(faabric_runner executor state endpoint)
