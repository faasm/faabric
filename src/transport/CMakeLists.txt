# ----------------------------------------------
# Faabric transport library: ZeroMQ + Protobuf
# ----------------------------------------------

faabric_lib(transport
    Message.cpp
    MessageEndpoint.cpp
    MessageEndpointClient.cpp
    MessageEndpointServer.cpp
    PointToPointBroker.cpp
    PointToPointClient.cpp
    PointToPointServer.cpp
)

target_link_libraries(transport PRIVATE
    faabric::util
    faabric::proto
    faabric::scheduling_util
)
