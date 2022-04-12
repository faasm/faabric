
faabric_lib(endpoint
    FaabricEndpoint.cpp
    FaabricEndpointHandler.cpp
)

target_link_libraries(endpoint PRIVATE faabric::util)
