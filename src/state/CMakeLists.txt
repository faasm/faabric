file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/state/*.h")

set(LIB_FILES
        InMemoryStateKeyValue.cpp
        InMemoryStateRegistry.cpp
        State.cpp
        StateClient.cpp
        StateKeyValue.cpp
        StateServer.cpp
        RedisStateKeyValue.cpp
        ${HEADERS}
        )

faabric_lib(state "${LIB_FILES}")

target_link_libraries(state proto redis transport)
