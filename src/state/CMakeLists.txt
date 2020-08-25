include_directories(
        ${FAABRIC_INCLUDE_DIR}/faabric/state
)

file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/state/*.h")

add_library(state
        SHARED
        DummyStateServer.cpp
        InMemoryStateKeyValue.cpp
        InMemoryStateRegistry.cpp
        State.cpp
        StateClient.cpp
        StateKeyValue.cpp
        StateServer.cpp
        RedisStateKeyValue.cpp
        ${HEADERS}
    )

target_link_libraries(state proto redis)
