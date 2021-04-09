file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/thread_pool/*.h")

set(LIB_FILES
        MpiThreadPool.cpp
        ${HEADERS}
        )

faabric_lib(thread_pool "${LIB_FILES}")

target_link_libraries(thread_pool util)
