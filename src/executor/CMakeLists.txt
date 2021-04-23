file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/executor/*.h")

set(LIB_FILES
        FaabricExecutor.cpp
        FaabricMain.cpp
        FaabricPool.cpp
        DummyExecutor.cpp
        ${HEADERS}
        )

faabric_lib(executor "${LIB_FILES}")

target_link_libraries(executor scheduler)

