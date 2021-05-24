file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/endpoint/*.h")

set(LIB_FILES
        Endpoint.cpp
        FaabricEndpoint.cpp
        FaabricEndpointHandler.cpp
        ${HEADERS}
        )

faabric_lib(endpoint "${LIB_FILES}")

target_link_libraries(endpoint pistache_imported pthread util)
