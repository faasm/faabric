file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/snapshot/*.h")

set(LIB_FILES
    SnapshotClient.cpp
    SnapshotRegistry.cpp
    SnapshotServer.cpp
    ${HEADERS}
)

faabric_lib(snapshot "${LIB_FILES}")

target_link_libraries(snapshot proto flat transport util)
