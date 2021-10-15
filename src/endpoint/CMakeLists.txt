
faabric_lib(endpoint
    Endpoint.cpp
    FaabricEndpointHandler.cpp
)

target_link_libraries(endpoint PRIVATE faabric::util)
