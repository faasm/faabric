# ----------------------------------------------
# Faabric transport library: NNG or raw TCP
# ----------------------------------------------

# Raw TCP sockets
add_subdirectory(tcp)

# NNG sockets
faabric_lib(transport
    Message.cpp
    MessageEndpoint.cpp
    MessageEndpointClient.cpp
    MessageEndpointServer.cpp
    # TODO: move to src/ptp
    PointToPointBroker.cpp
    PointToPointClient.cpp
    PointToPointServer.cpp
)

target_link_libraries(transport PRIVATE
    faabric::transport_tcp
    faabric::util
    faabric::proto
    faabric::scheduling_util
)
