
faabric_lib(endpoint
    Endpoint.cpp
    FaabricEndpoint.cpp
    FaabricEndpointHandler.cpp
)

target_link_libraries(endpoint PRIVATE faabric::util)
