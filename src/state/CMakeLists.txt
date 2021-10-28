
faabric_lib(state
    InMemoryStateKeyValue.cpp
    InMemoryStateRegistry.cpp
    State.cpp
    StateClient.cpp
    StateKeyValue.cpp
    StateServer.cpp
    RedisStateKeyValue.cpp
)

target_link_libraries(state PRIVATE
    faabric::proto
    faabric::redis
    faabric::transport
)
