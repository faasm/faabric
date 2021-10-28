
faabric_lib(snapshot
    SnapshotClient.cpp
    SnapshotRegistry.cpp
    SnapshotServer.cpp
)

target_link_libraries(snapshot PRIVATE
    faabric::proto
    faabric::flat
    faabric::transport
    faabric::util
)
